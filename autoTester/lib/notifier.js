export default class Notifier {
  constructor() {}
  async alertFinished(url, message, exitCode) {
    throw 'Not implemented';
  }
  async alertError(url, message) {
    throw 'Not implemented';
  }
  async alertStarted(url) {
    throw 'Not implemented';
  }
}
