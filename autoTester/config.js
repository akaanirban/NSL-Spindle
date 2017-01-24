const config = {
  checkMs: 30 * 1000,
  //sbtArgs: ['small:test'],
  sbtArgs: ['dev:test'],
  workdir: '../',
  env: {
    'LOCAL_DOCKER': 'true'
  },
  logsDir: 'logs',
  expressPort: 5050,
  sns: {
    region: 'us-east-1',
    targetArn: 'arn:aws:sns:us-east-1:500518139216:Rory',
  }
};

export default config;
