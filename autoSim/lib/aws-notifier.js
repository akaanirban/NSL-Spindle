import AWS from 'aws-sdk';
import Notifier from './notifier';
import config from '../config';

const {targetArn, region} = config.sns;

AWS.config.update({region});

console.log('target arn', targetArn);

export default class SnsNotifier extends Notifier {
  constructor() {
    super();
    this._sns = new AWS.SNS();
  }
  _sendMessage(subject, message) {
    return new Promise((resolve) => this._sns.publish({
      TargetArn: targetArn,
      Message: message,
      Subject: subject
    }, (err, data) => {
      if(err) {
        throw err;
      }
      resolve(data);
    }));
  }
  async alertFinished(config, message, exitCode) {
    await this._sendMessage(`Test finished ${exitCode}`, `${message}\n${config}`);
  }
  async alertError(config, message) {
    await this._sendMessage('Test error', `${message}\n${config}`);
  }
}
