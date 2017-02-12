'use strict';
import 'babel-polyfill';
import {spawn} from 'child_process';
import express from 'express';
import winston from 'winston';
import config from '../config';
import DatabaseClient from './pgClient';

const mkLogger = (fileName) => new (winston.Logger)({
  level: 'debug',
  transports: [
    new (winston.transports.Console)(),
    new (winston.transports.File)({filename: `${config.logsDir}/${fileName}.log`})
  ],
});

const metaLogger = mkLogger('meta');

const app = express();

const dbClient = new DatabaseClient();

function _mapToTable({clustertable, numnodes, windowsizems, maxiterations, num_tests, avg_bytes}) {
    return `
        <tr>
            <td>${clustertable}</td>
            <td>${numnodes}</td>
            <td>${windowsizems}</td>
            <td>${maxiterations}</td>
            <td>${num_tests}</td>
            <td>${parseFloat(avg_bytes).toFixed(2)}</td>
        </tr>`;

}

async function _showResults(req, res) {
    const results = await dbClient.getResults();
    console.log(results);
    const resultTableHead = `
        <table border="1">
            <tr>
                <th>Cluster Table</th>
                <th>Number of Nodes</th>
                <th>Reduce Window Ms</th>
                <th>Max Time Steps Per Vehicle</th>
                <th>Number of Tests</th>
                <th>Average Bytes to Middleware</th>
            </tr>`;
    const resultTableBody = results.map(_mapToTable).join('');
    const resultTableTail = `</table>`;
    const htmlDoc = `
        <!DOCTYPE html>
        <html>
            <head>
                <title>Spindle Sim Results</title>
                <meta http-equiv="refresh" content="20" />
            </head>
            <body>${resultTableHead}${resultTableBody}${resultTableTail}</body>
        </html>`.split('\n').map(line => line.trim()).join('\n');
    return res.send(htmlDoc);
}

app.get('/', _showResults);

app.listen(config.expressPort, () => {
    winston.info(`Started server on ${config.expressPort}`);
});
