import fs from 'fs';
import path from 'path';
import express from 'express';
import serveIndex from 'serve-index';
import serveStatic from 'serve-static';
import config from '../config';

const app = express();

app.get('/', (req, res) => {
  return res.status(404).end();
});

const logsPath = path.join(config.logsDir);
console.log('logs path', logsPath);
app.use('/logs', serveIndex(logsPath));
//app.use('/logs', serveStatic(logsPath));

const parseFile = (fileContents) => {
  return `<ul>${String(fileContents)
  .split('\n')
  .map(line => line.trim())
  .filter(line => line.length > 0)
  .map(line => {
    console.log('parsing line', line);
    return JSON.parse(line);
  })
  .map(({level, message, timestamp}) => `<li><b>${level} (${timestamp})</b> ${message}</li>`)
  .join('')}</ul>`;
}

async function loadFile(file) {
  const fileContents = await new Promise((resolve) => {
    fs.readFile(path.join(logsPath, file), (err, contents) => {
      if(err){ throw err; }
      resolve(contents);
    });
  });
  console.log('file', String(fileContents));
  return `<html><body>${parseFile(fileContents)}</body></html>`;
}

app.get('/logs/:file', (req, res) => {
  const {file} = req.params;
  return loadFile(file).then(html => res.send(html));
});

export default app;
