#!/usr/bin/env node
const fs = require('fs');
const spawn = require('child_process').spawn;


function openLog() {
    return fs.openSync('./daemon.log', 'a');
}

console.log('Starting daemon');
const child = spawn('node', ['./index.js'].concat(process.argv.slice(2)), {
    detached: true,
    stdio: ['ignore', openLog(), openLog()],
});
child.unref(); // Detach

console.log('Forked', child);
