import pgp from 'pg-promise';
import config from '../config';

class Table {
  constructor(name, initScript) {
    this.name = name;
    this.initScript = initScript;
  }
}

const _mkSimResults = `CREATE TABLE IF NOT EXISTS sim_results(
  jobUUID varchar(40) NOT NULL REFERENCES sim_runs(jobUUID),
  dest varchar(500) NOT NULL,
  source varchar(500) NOT NULL,
  numBytes INT NOT NULL,
  PRIMARY KEY(jobUUID, dest, source)
)`;

const _mkSimRuns = `CREATE TABLE IF NOT EXISTS sim_runs(
  configId INT REFERENCES sim_configs(configId), 
  jobUUID varchar(40) NOT NULL PRIMARY KEY
)`;

const _mkSimConfigs = `CREATE TABLE IF NOT EXISTS sim_configs(
  configId SERIAL PRIMARY KEY,
  clusterTable varchar(100) NOT NULL,
  numNodes INT NOT NULL,
  maxIterations INT NOT NULL DEFAULT 20,
  mapReduceName varchar(500) NOT NULL,
  description text,
  UNIQUE(clusterTable, numNodes, mapReduceName, maxIterations)
)`;

const _tables = {
  configs: new Table('sim_configs', _mkSimConfigs),
  runs: new Table('sim_runs', _mkSimRuns),
  results: new Table('sim_results', _mkSimResults),
};

const _initTables = (client) => {
  // Foreign keys require ordered references
  const tables = [_tables.configs, _tables.runs, _tables.results]; 
  return tables.reduce((prev, table) => {
    return prev.then(() => {
      return client.runSql(table.initScript, []);
    });
  }, Promise.resolve(null));
};

const _getLeastTested = `
  SELECT cnts.runcount, cfg.*
  FROM 
    (SELECT cfg.configId, COUNT(jobUUID) as runcount
    FROM sim_configs cfg
    LEFT JOIN sim_runs run
      ON cfg.configId = run.configId
    GROUP BY cfg.configId) as cnts
  INNER JOIN sim_configs cfg
    ON cfg.configId = cnts.configId
  ORDER BY cnts.runcount asc
`;

const _insertRun = `
INSERT INTO sim_runs(configid, jobuuid)
VALUES ($1, $2)
`;

const _insertResult = `
INSERT INTO sim_results(jobuuid, source, dest, numbytes)
VALUES ($1, $2, $3, $4)
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
  async getNextConfig() {
    const data = await this.runSql(_getLeastTested, []);
    const [nextRow,] = data;
    const keys = Object.keys(nextRow)
      .map(key => ({oldKey:key, newKey: key.replace('\'', '')}));
    return keys.reduce((obj, {oldKey, newKey}) => {
      const value = nextRow[oldKey];
      obj[newKey] = value;
      return obj;
    }, {});
  }
  async addResults(configId, jobUid, results) {
    await this.runSql(_insertRun, [configId, jobUid]);
    const insertPromises = results.map(({dest, source, numBytes}) => {
      return this.runSql(_insertResult, [jobUid, source, dest, numBytes]);
    });
    await Promise.all(insertPromises);
  }
}

export default DatabaseClient;
