import pgp from 'pg-promise';
import config from '../config';

const _getResults = `
    SELECT * 
    FROM results_middleware_avg_bytes
    ORDER BY numnodes, windowsizems, maxiterations, avg_bytes asc
`;

class DatabaseClient {
  constructor() {
    const {pgConnection} = config;
    this._db = pgp()(pgConnection);
  }
  initTables() {
    return _initTables(this);
  }
  runSql(sql, params) {
    return this._db.any(sql, params).catch((err) => {
      console.error('Sql statement failed', sql, params, err);
      throw err;
    });
  }
  getResults() { 
    return this.runSql(_getResults, []);
  }
}

export default DatabaseClient;
