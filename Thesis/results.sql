-- Associate results with configurations
DROP VIEW annotated_results CASCADE;
CREATE VIEW annotated_results AS
SELECT configs.*, res.* 
FROM 
    sim_results_v5 res,
    sim_runs_v5 runs,
    sim_configs_v5 configs
WHERE
    runs.configid=configs.configid and
    runs.jobuuid=res.jobuuid
;


DROP VIEW node_counts;
CREATE MATERIALIZED VIEW node_counts AS
(SELECT 'sparse_positions' filtertablename, count(distinct node) numnodes from "sparse_positions")
UNION
(SELECT 'dense_positions' filtertablename, count(distinct node) numnodes from "dense_positions")
;

-- Get sum of bytes per run
DROP VIEW job_middleware_bytes CASCADE;
CREATE VIEW job_middleware_bytes AS
SELECT jobuuid, SUM(numbytes) total_bytes_to_middleware
FROM sim_results_v5 
WHERE dest='middleware-input'
GROUP BY jobuuid;

DROP VIEW annotated_runs CASCADE;
CREATE VIEW annotated_runs AS
SELECT configs.*, total_bytes_to_middleware, total_bytes_to_middleware / node_counts.numnodes as avg_bytes_to_middleware, node_counts.numnodes true_num_nodes
FROM 
    sim_runs_v5 runs,
    sim_configs_v5 configs,
    job_middleware_bytes jobbytes,
    node_counts
WHERE
    runs.configid=configs.configid and
    runs.jobuuid=jobbytes.jobuuid and
    configs.filtertablename=node_counts.filtertablename
;

DROP VIEW simple_annotated_runs CASCADE;
CREATE VIEW simple_annotated_runs AS
SELECT configid, clustertable, filtertablename, windowsizems, mapreducename, total_bytes_to_middleware, true_num_nodes
FROM annotated_runs
;

DROP VIEW byte_averaged_runs CASCADE;
CREATE VIEW byte_averaged_runs AS
SELECT configid, clustertable, filtertablename, windowsizems, mapreducename, avg(total_bytes_to_middleware) avg_total_bytes 
FROM simple_annotated_runs
GROUP BY configid, clustertable, filtertablename, windowsizems, mapreducename;

DROP VIEW normed_runs CASCADE;
CREATE VIEW normed_runs AS
SELECT b.*, b.avg_total_bytes / normref.avg_total_bytes normed_total_bytes
FROM byte_averaged_runs b, byte_averaged_runs normref
WHERE
    normref.clustertable='self_clusters' and
    normref.filtertablename=b.filtertablename and
    normref.mapreducename=b.mapreducename and
    normref.windowsizems=b.windowsizems
;
