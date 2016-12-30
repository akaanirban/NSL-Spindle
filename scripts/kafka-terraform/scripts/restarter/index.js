#!/usr/bin/env node
const fs = require('fs');
const spawn = require('child_process').spawn;

const command = process.argv[2];
const args = (() => {
    if(process.argv.length == 3) {
        return [];
    }
    return process.argv.slice(3);
})();

const logPath = 'nsl-process.log';

(function runCommand() {
    console.log('Running', command, args);
    const logFile = fs.createWriteStream(logPath, {flags: 'a'});
    const process = spawn(command, args);
    process.stdout.pipe(logFile);
    process.stderr.pipe(logFile);

    process.on('exit', (code) => {
        console.error('Program crashed', code);
        fs.appendFile(logPath, `Program crashed ${new Date()}\n`, () => {});
        setTimeout(runCommand, 1000);
    });
})();
