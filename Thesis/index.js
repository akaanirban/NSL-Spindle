const watch = require('watch');
const exec = require('child_process').exec;

const filePath = 'rpithes-short.tex';
const pdfPath = 'rpithes-short.pdf';

watch.createMonitor('./', (monitor) => {
    monitor.files[filePath];
    monitor.on('changed', (f, stat) => {
        if(f.endsWith('.mmd')){
            exec(`mermaid --width 1920 ${f}`, (err) => {
                if(err) { throw err; }
                console.log('Compiled mermaid', f);
            });
            return;
        } else if(f !== filePath) {
            return;
        }
        exec(`pdflatex ${filePath}`, (err, stdout, stderr) => {
            if(err) { throw err; }
            exec(`open ${pdfPath}`);
        });
    });
});
