import pgp from 'pg-promise';
import config from '../config';

class Table {
  constructor(name, initScript) {
    this.name = name;
    this.initScript = initScript;
  }
}

const tablesVersion = 'v4';

const _mkSimResults = `CREATE TABLE IF NOT EXISTS sim_results_${tablesVersion}(
  jobUUID varchar(40) NOT NULL REFERENCES sim_runs_${tablesVersion}(jobUUID),
  dest varchar(500) NOT NULL,
  source varchar(500) NOT NULL,
  numBytes INT NOT NULL,
  PRIMARY KEY(jobUUID, dest, source)
)`;

const _mkSimRuns = `CREATE TABLE IF NOT EXISTS sim_runs_${tablesVersion}(
  configId INT REFERENCES sim_configs_${tablesVersion}(configId), 
  jobUUID varchar(40) NOT NULL PRIMARY KEY,
  timestamp timestamp DEFAULT current_timestamp
)`;

const _mkSimConfigs = `CREATE TABLE IF NOT EXISTS sim_configs_${tablesVersion}(
  configId SERIAL PRIMARY KEY,
  clusterTable varchar(100) NOT NULL,
  numNodes INT NOT NULL,
  windowSizeMs BIGINT NOT NULL, 
  maxIterations INT NOT NULL DEFAULT 20000,
  enabled BOOLEAN DEFAULT TRUE,
  --Name of map reduce configuration--
  mapReduceName varchar(500) NOT NULL,
  filterTableName varchar(200) NOT NULL DEFAULT 'all_nodes',
  description text
)`;

const _tables = {
  configs: new Table(`sim_configs_${tablesVersion}`, _mkSimConfigs),
  runs: new Table(`sim_runs_${tablesVersion}`, _mkSimRuns),
  results: new Table(`sim_results_${tablesVersion}`, _mkSimResults),
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
    FROM sim_configs_${tablesVersion} cfg
    LEFT JOIN sim_runs_${tablesVersion} run
      ON cfg.configId = run.configId
    GROUP BY cfg.configId) as cnts
  INNER JOIN sim_configs_${tablesVersion} cfg
    ON cfg.configId = cnts.configId
    -- NOTE: filter inadequate iteration count --
  WHERE cfg.enabled = TRUE and cfg.numnodes <= ${config.maxVehicles}
  ORDER BY cnts.runcount asc
`;

const _insertRun = `
INSERT INTO sim_runs_${tablesVersion}(configid, jobuuid)
VALUES ($1, $2)
`;

const _insertResult = `
INSERT INTO sim_results_${tablesVersion}(jobuuid, source, dest, numbytes)
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
