const config = {
  sbtArgs: ['-mem', '92160', 'run'],
  workdir: '../',
  env: process.env,
  logsDir: 'logs',
  appConfPath: '../src/main/resources/application.conf',
  appResultsDir: 'simulation-results/completed',
  sns: {
    region: 'us-east-1',
    targetArn: 'arn:aws:sns:us-east-1:500518139216:nsl-spindle-sim',
  },
  s3Bucket: 'spindle-results',
  maxVehicles: process.env.MAX_VEHICLES || 10000,
  pgConnection: 'postgres://postgres:spindle@ec2-184-73-126-96.compute-1.amazonaws.com:5432/postgres' 
};

export default config;
