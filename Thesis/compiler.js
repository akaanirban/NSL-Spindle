const watch = require('watch');
const exec = require('child_process').exec;

const filePath = 'rpithes-short.tex';
const pdfPath = 'rpithes-short.pdf';
const binDir = './binimages';
const datavisR = 'datavis.r';

watch.createMonitor('./', (monitor) => {
    monitor.files[filePath];
    monitor.on('changed', (f, stat) => {
        if(f.endsWith('.mmd')){
            var cssArg = '--css mermaid.css';
            if(f.toLowerCase().indexOf('sequence') == -1) { cssArg = ''; }
            exec(`mermaid ${cssArg} --width 1920 --outputDir ${binDir} ${f}`, (err) => {
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
        } else if(f === datavisR) {
          return exec(`Rscript ${f}`, (err, stdout, stderr) => {
            if(err) {
              return console.error(stdout, stderr);
            }
          });  
        } else if(f !== filePath) {
            return;
        }
        exec(`pdflatex -halt-on-error ${filePath}`, (err, stdout, stderr) => {
            if(err) { 
                return console.error(stdout, stderr);
            }
            console.log('Compiled tex');
            return;
            console.log('Opening file');
            exec(`open ${pdfPath}`, (err, stdout, stderr) => {
                if(err) { throw err;}
                console.log(stdout);
                console.error(stderr);
            });
        });
    });
});
