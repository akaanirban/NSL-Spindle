import fs from 'fs';
import AWS from 'aws-sdk';
import config from '../config';

const _s3 = new AWS.S3();

const {s3Bucket} = config;

console.log('s3 bucket', s3Bucket);

const uploadFile = ({s3Path, filePath}) => {
    const params = {
        Bucket: s3Bucket,
        Key: s3Path,
        Body: fs.readFileSync(filePath),
    };
    return new Promise((resolve, reject) => {
        _s3.putObject(params, (err, data) => {
            if(err) { throw err; }
            console.log('Uploaded', filePath, data);
            return resolve(data);
        }); 
    });
}

async function uploadDir(jobUid, path) {
    const filePaths = fs.readdirSync(path).map(fileName => ({filePath: `${path}/${fileName}`, s3Path: `${jobUid}/${fileName}`,}));
    console.log(filePaths);
    await Promise.all(filePaths.map(uploadFile));
    console.log('Files uploaded');
}
export default uploadDir;
