'use strict';
import 'babel-polyfill';
import {spawn} from 'child_process';
import winston from 'winston';
import uuid from 'uuid/v4';
import kill from 'tree-kill';
import config from '../config';
import fs from 'fs';
import rmrf from 'rmrf';
import DatabaseClient from './pgClient';
import Notifier from './aws-notifier';
import uploadDir from './s3Uploader';

const mkLogger = (fileName) => new (winston.Logger)({
  level: 'debug',
  transports: [
    new (winston.transports.Console)(),
    new (winston.transports.File)({filename: `${config.logsDir}/${fileName}.log`})
  ],
});

const notifier = new Notifier();

const metaLogger = mkLogger('meta');

function runSim() {
  const {sbtArgs, workdir, env} = config;

  metaLogger.info('Launching process');
  const process = spawn('sbt', sbtArgs, {cwd: workdir, env});

  const startMessage = 'Starting sim';
  const finishMessage = 'Program Finished. Exiting NSL-Spindle Simulator.';
  const finishingMessage = 'Program Finishing. Closing down NSL-Spindle Simulator';

  var finished = false;

  process.stderr.on('data', data => console.log('stderr', String(data)));

  const simUidPromise = new Promise((resolve) => {
    process.stdout.on('data', (data) => {
      const strData = String(data);
      if(strData.indexOf(startMessage) !== -1) {
        const [startLine, ] = strData.split('\n').filter(line => line.indexOf(startMessage) !== -1);
        const [, , simUid] = startLine.split(' ');
        return resolve(simUid);
      }
    });
    process.on('end', () => resolve(null));
  });
  const simFinishedPromise = new Promise((resolve, reject) => {
    process.stderr.on('data', (data) => {
      const strData = String(data);
      console.log('stdout', strData);
      if(strData.indexOf(finishMessage) !== -1) {
        metaLogger.info(`Got finished message. Killing process ${process.pid}`);
        kill(process.pid, 'SIGKILL');
        finished = true;
        return resolve(null);
      }
      if(strData.indexOf(finishingMessage) !== -1) {
        finished = true;
        metaLogger.info(`Got finishing message. Waiting for process completion`);
      }
    });
    process.on('close', () => {
      finished = true;
      return resolve(null);
    });
    // watch for failures
    process.stdout.on('data', (data) => {
      const strData = String(data).toLowerCase();
      if(strData.indexOf('exception') !== -1) {
        if(finished === false) {
          metaLogger.error(strData);
          metaLogger.error('Exception detected. Killing process');
          kill(process.pid, 'SIGKILL');
          finished = true;
          return reject(strData);
        }
      } else if(strData.indexOf('error') !== -1) {
        metaLogger.warn(strData);
      }
      if(strData.indexOf('assertionerror') !== -1) {
        if(finished === false) {
          kill(process.pid, 'SIGKILL');
          finished = true;
          return reject(strData);
        }
      }
    });
  });
  const simShutdownPromise = new Promise((resolve) => {
    process.on('close', () => {
      finished = true;
      return resolve(null);
    });
  });

  return {simUidPromise, simFinishedPromise, simShutdownPromise};
}

//TODO: make conf
function writeConfig(nextConfig, resultsDir) {
  const {runcount, clustertable, numnodes, maxiterations, mapreducename, windowsizems} = nextConfig;
  const resultsDirParam = 'simulation.results.dir';
  const clustertableParam = 'spindle.sim.vehicle.cluster.member.table';
  const maxVehiclesParam = 'spindle.sim.vehicle.max.vehicles';
  const maxIterationsParam = 'spindle.sim.vehicle.max.iterations';
  const mapReduceNameParam = 'spindle.sim.vehicle.mapreduce.config.name';
  const windowSizeParam = 'spindle.sim.streams.reduce.window.ms';

  const resultsDirSetting = `${resultsDirParam} = "${resultsDir}"`;
  const clusterTableSetting = `${clustertableParam} = "${clustertable}"`;
  const maxVehiclesSetting = `${maxVehiclesParam} = ${numnodes}`;
  const maxIterationsSetting = `${maxIterationsParam} = ${maxiterations}`;
  const mapReduceNameSetting = `${mapReduceNameParam} = "${mapreducename}"`;
  const windowSizeSetting = `${windowSizeParam} = ${windowsizems}`;

  const configContents = String(fs.readFileSync(config.appConfPath))
    .split('\n').map(line => {
      if(line.indexOf(maxVehiclesParam) != -1) {
        return maxVehiclesSetting;
      } else if(line.indexOf(clustertableParam) != -1) {
        return clusterTableSetting;
      } else if(line.indexOf(resultsDirParam) != -1) {
        return resultsDirSetting;
      } else if(line.indexOf(maxIterationsParam) != -1) {
        return maxIterationsSetting; 
      } else if (line.indexOf(mapReduceNameParam) != -1) {
        return mapReduceNameSetting;
      } else if(line.indexOf(windowSizeParam) != -1) {
        return windowSizeSetting;
      }
      return line;
  }).join('\n');
  fs.writeFileSync(config.appConfPath, configContents);
  metaLogger.info('Wrote new config file');
}

function parseResults(resultsDir) {
  const dataPrefix = 'data-sent';
  const channelRegex = /data\-sent\-to\-sim\-([0-9a-z]+\-){5}([0-9a-z\-]+)\-from-(.*).csv/;

  const dataFiles = fs.readdirSync(resultsDir).filter(file => file.indexOf(dataPrefix) !== -1); 

  const extractChannels = (file) => {
    const [, , dest, source] = channelRegex.exec(file);
    return {dest, source, file};
  };

  const getData = (metadata) => {
    const {file} = metadata;
    const path = `${resultsDir}/${file}`;
    const numBytes = String(fs.readFileSync(path)).trim().split('\n').slice(-1)[0].split(',')[1];
    delete metadata.file;
    metadata.numBytes = numBytes;
    return metadata;
  };

  return dataFiles.map(extractChannels).map(getData);
}

async function runNextSim(client) {
  await client.initTables();
  metaLogger.info('Initialized tables');
  const nextConfig = await client.getNextConfig();
  metaLogger.info('next config', nextConfig);
  const resultsDir = `${config.appResultsDir}/${nextConfig.clustertable}_${nextConfig.numnodes}nodes_${nextConfig.maxiterations}iterations_run${nextConfig.runcount}`;
  // Autorunner and simulator run from different directories
  const resultsDirRelative = `../${resultsDir}`;
  rmrf(resultsDirRelative);
  writeConfig(nextConfig, resultsDir);
  const {simUidPromise, simFinishedPromise, simShutdownPromise} = runSim();
 
  try {
    const simUid = await simUidPromise;
    metaLogger.info(`Sim UID: ${simUid}`);
    await simFinishedPromise;
    metaLogger.info("Parsing results");
    const results = parseResults(resultsDirRelative);
    console.log('results', results);
    //TODO: Store logs in row
    await uploadDir(simUid, resultsDirRelative);
    await client.addResults(nextConfig.configid, simUid, results); 
    notifier.alertFinished(JSON.stringify(nextConfig), JSON.stringify(results));
  } catch(err) { 
    metaLogger.error(`Sim failed: ${err}`, nextConfig);
    notifier.alertError(JSON.stringify(nextConfig), JSON.stringify(err));
  }
  metaLogger.info('Waiting for sim full shutdown');
  await simShutdownPromise;
  metaLogger.info('Sim has fully shutdown');
}

async function runSims() {
  const client = new DatabaseClient();
  async function callRunner() {
    runNextSim(client).then(() => {
      metaLogger.info("Sim ended. Calling next.");
      setTimeout(callRunner, 5000);
    });
  }
  callRunner();
}

runSims();
