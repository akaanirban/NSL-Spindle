const watch = require('watch');
const exec = require('child_process').exec;

const filePath = 'rpithes-short.tex';
const pdfPath = 'rpithes-short.pdf';
const binDir = './binimages';

watch.createMonitor('./', (monitor) => {
    monitor.files[filePath];
    monitor.on('changed', (f, stat) => {
        if(f.endsWith('.mmd')){
            exec(`mermaid --width 1920 --outputDir ${binDir} ${f}`, (err) => {
                if(err) { throw err; }
                console.log('Compiled mermaid', f);
                const [, fileNameWithExt] = f.split('/');
                const [fileName, ] = fileNameWithExt.split('.');
                exec(`mv ${binDir}/${fileNameWithExt}.png ${binDir}/${fileName}.png`, (err) => {
                    if(err) { throw err; }
                    console.log('moved mermaid');
                });
            });
            return;
        } else if(f !== filePath) {
            console.log('Skipping file', f);
            return;
        }
        exec(`pdflatex -halt-on-error ${filePath}`, (err, stdout, stderr) => {
            if(err) { 
                return console.error(stdout, stderr);
            }
            console.log('Compiled tex. Opening file');
            exec(`open ${pdfPath}`, (err, stdout, stderr) => {
                if(err) { throw err;}
                console.log(stdout);
                console.error(stderr);
            });
        });
    });
});
