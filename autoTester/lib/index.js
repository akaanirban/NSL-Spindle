'use strict';
import 'babel-polyfill';
import {spawn} from 'child_process';
import simpleGit from 'simple-git';
import winston from 'winston';
import config from '../config';
import app from  './server';
import Notifier from './aws-notifier';

const mkLogger = (fileName) => new (winston.Logger)({
  level: 'debug',
  transports: [
    new (winston.transports.Console)(),
    new (winston.transports.File)({filename: `${config.logsDir}/${fileName}.log`})
  ],
});

const notifier = new Notifier();

const metaLogger = mkLogger('meta');

const git = simpleGit(config.workdir);

const handleError = (error, url = 'no url specified') => {
  metaLogger.error('Error', error);
  notifier.alertError(url, 'Failure during test', JSON.stringify(error));
  throw error;
}

// Check if a pull is required
const checkPull = () => {
  return new Promise((resolve) => {
    git.fetch((err, summary) => {
      if(err) { handleError(err); }
      //metaLogger.debug('fetch summary', summary);
      git.status((err, statusSummary) => {
        if(err) { handleError(err); }
        //metaLogger.debug('status', statusSummary);
        resolve(statusSummary.behind > 0);
      });
    });
  });
}

const doPull = () => {
  return new Promise((resolve) => git.pull((err) => {
    if(err) { handleError(err); }
    resolve(null);
  })); 
}

const getLog = () => {
  return new Promise((resolve) => git.log((err, log) => {
    if(err) { handleError(err); }
    resolve(log);
  }));
}

async function needsToTest(prevHash) {
 const needPull = await checkPull();
  if(needPull) {
    await doPull();
  }
  // Compare commit hashes
  const {latest: {hash}} = await getLog();
  return {doTest: hash !== prevHash, latestHash: hash};
}

async function runTests(testLogger, url) {
  const {sbtArgs, workdir, env} = config;
  const process = spawn('sbt', sbtArgs, {cwd: workdir, env});
  process.stdout.on('data', (data) => testLogger.info(String(data)));
  process.stderr.on('data', (data) => testLogger.warn(String(data)));
  return new Promise((resolve) => {
    process.on('close', (returnCode) => {
      const failed = returnCode !== 0;
      metaLogger.debug('test returned code', returnCode);
      if(failed) {
        testLogger.error('Test failed');
        notifier.alertError(url, `Test failed with return code ${returnCode}`); 
      } else {
        testLogger.info('Tests passed');
      }
      notifier.alertFinished(url, `SBT command ${sbtArgs} completed`, returnCode);
      resolve({returnCode, failed});
    });
  });
}

const mkUrl = (logPath) => `http://${process.env.HOSTNAME}:${config.expressPort}/logs/${logPath}.log`;

// Run test if changes have occurred
async function tryTest(lastTestedHash = null) {
  const {doTest, latestHash} = await needsToTest(lastTestedHash);
  if(doTest) {
    const logPath = `test-${String(new Date()).replace(/ /g, '_')}-commit-${latestHash}`;
    const testLogger = mkLogger(logPath);
    const url = mkUrl(logPath);
    notifier.alertStarted(url);
    await runTests(testLogger, url);
  }
  setTimeout(() => tryTest(latestHash), config.checkMs);
}

tryTest();


app.listen(config.expressPort, () => {
  metaLogger.info('Express server started');
});
