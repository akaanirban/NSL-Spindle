const fs = require('fs');
const spawn = require('child_process').spawn;

const logFile = fs.openSync('compiler.log', 'w');

const proc = spawn('node', ['compiler.js'], {
    stdio: ['ignore', logFile, logFile],
    detached: true
});

fs.writeFileSync('compiler.pid', `${proc.pid}`);

proc.unref();
