-- DELETE FROM sim_configs_v5;

-- Dense config

INSERT INTO sim_configs_v5(configId, numNodes, clusterTable, mapReduceName, windowSizeMs, filtertablename)
VALUES (1, 1000, 'clusterinfo_dense_v1', 'speedSum', 10000, 'dense_positions');

INSERT INTO sim_configs_v5(configId, numNodes, clusterTable, mapReduceName, windowSizeMs, filtertablename)
VALUES (2, 1000, 'clusterinfo_dense_v1', 'speedSum', 15000, 'dense_positions');

INSERT INTO sim_configs_v5(configId, numNodes, clusterTable, mapReduceName, windowSizeMs, filtertablename)
VALUES (3, 1000, 'clusterinfo_dense_v1', 'speedSum', 30000, 'dense_positions');

-- Geo filtered queries

INSERT INTO sim_configs_v5(configId, numNodes, clusterTable, mapReduceName, windowSizeMs, filtertablename)
VALUES (4, 1000, 'clusterinfo_dense_v1', 'geoFiltered', 10000, 'dense_positions');

INSERT INTO sim_configs_v5(configId, numNodes, clusterTable, mapReduceName, windowSizeMs, filtertablename)
VALUES (5, 1000, 'clusterinfo_dense_v1', 'geoFiltered', 15000, 'dense_positions');

INSERT INTO sim_configs_v5(configId, numNodes, clusterTable, mapReduceName, windowSizeMs, filtertablename)
VALUES (6, 1000, 'clusterinfo_dense_v1', 'geoFiltered', 30000, 'dense_positions');

INSERT INTO sim_configs_v5(configId, numNodes, clusterTable, mapReduceName, windowSizeMs, filtertablename)
VALUES (34, 1000, 'self_clusters', 'geoFiltered', 30000, 'dense_positions');

INSERT INTO sim_configs_v5(configId, numNodes, clusterTable, mapReduceName, windowSizeMs, filtertablename)
VALUES (35, 1000, 'self_clusters', 'geoFiltered', 15000, 'dense_positions');

INSERT INTO sim_configs_v5(configId, numNodes, clusterTable, mapReduceName, windowSizeMs, filtertablename)
VALUES (36, 1000, 'self_clusters', 'geoFiltered', 10000, 'dense_positions');

-- Geo-mapped queries

INSERT INTO sim_configs_v5(configId, numNodes, clusterTable, mapReduceName, windowSizeMs, filtertablename)
VALUES (7, 1000, 'clusterinfo_dense_v1', 'geoMapped', 10000, 'dense_positions');

INSERT INTO sim_configs_v5(configId, numNodes, clusterTable, mapReduceName, windowSizeMs, filtertablename)
VALUES (8, 1000, 'clusterinfo_dense_v1', 'geoMapped', 15000, 'dense_positions');

INSERT INTO sim_configs_v5(configId, numNodes, clusterTable, mapReduceName, windowSizeMs, filtertablename)
VALUES (9, 1000, 'clusterinfo_dense_v1', 'geoMapped', 30000, 'dense_positions');

INSERT INTO sim_configs_v5(configId, numNodes, clusterTable, mapReduceName, windowSizeMs, filtertablename)
VALUES (25, 1000, 'self_clusters', 'geoMapped', 10000, 'dense_positions');

INSERT INTO sim_configs_v5(configId, numNodes, clusterTable, mapReduceName, windowSizeMs, filtertablename)
VALUES (26, 1000, 'self_clusters', 'geoMapped', 15000, 'dense_positions');

INSERT INTO sim_configs_v5(configId, numNodes, clusterTable, mapReduceName, windowSizeMs, filtertablename)
VALUES (27, 1000, 'self_clusters', 'geoMapped', 30000, 'dense_positions');


INSERT INTO sim_configs_v5(configId, numNodes, clusterTable, mapReduceName, windowSizeMs, filtertablename)
VALUES (28, 1000, 'self_clusters', 'geoMapped', 10000, 'sparse_positions');

INSERT INTO sim_configs_v5(configId, numNodes, clusterTable, mapReduceName, windowSizeMs, filtertablename)
VALUES (29, 1000, 'self_clusters', 'geoMapped', 15000, 'sparse_positions');

INSERT INTO sim_configs_v5(configId, numNodes, clusterTable, mapReduceName, windowSizeMs, filtertablename)
VALUES (30, 1000, 'self_clusters', 'geoMapped', 30000, 'sparse_positions');

INSERT INTO sim_configs_v5(configId, numNodes, clusterTable, mapReduceName, windowSizeMs, filtertablename)
VALUES (31, 1000,  'clusterinfo_sparse_v1', 'geoMapped', 10000, 'sparse_positions');

INSERT INTO sim_configs_v5(configId, numNodes, clusterTable, mapReduceName, windowSizeMs, filtertablename)
VALUES (32, 1000,  'clusterinfo_sparse_v1', 'geoMapped', 15000, 'sparse_positions');

INSERT INTO sim_configs_v5(configId, numNodes, clusterTable, mapReduceName, windowSizeMs, filtertablename)
VALUES (33, 1000,  'clusterinfo_sparse_v1', 'geoMapped', 30000, 'sparse_positions');
-- Sparse config

INSERT INTO sim_configs_v5(configId, numNodes, clusterTable, mapReduceName, windowSizeMs, filtertablename)
VALUES (10, 1000, 'clusterinfo_sparse_v1', 'speedSum', 10000, 'sparse_positions');

INSERT INTO sim_configs_v5(configId, numNodes, clusterTable, mapReduceName, windowSizeMs, filtertablename)
VALUES (11, 1000, 'clusterinfo_sparse_v1', 'speedSum', 15000, 'sparse_positions');

INSERT INTO sim_configs_v5(configId, numNodes, clusterTable, mapReduceName, windowSizeMs, filtertablename)
VALUES (12, 1000, 'clusterinfo_sparse_v1', 'speedSum', 30000, 'sparse_positions');

-- Geo filtered queries

INSERT INTO sim_configs_v5(configId, numNodes, clusterTable, mapReduceName, windowSizeMs, filtertablename)
VALUES (13, 1000, 'clusterinfo_sparse_v1', 'geoFiltered', 10000, 'sparse_positions');

INSERT INTO sim_configs_v5(configId, numNodes, clusterTable, mapReduceName, windowSizeMs, filtertablename)
VALUES (14, 1000, 'clusterinfo_sparse_v1', 'geoFiltered', 15000, 'sparse_positions');

INSERT INTO sim_configs_v5(configId, numNodes, clusterTable, mapReduceName, windowSizeMs, filtertablename)
VALUES (15, 1000, 'clusterinfo_sparse_v1', 'geoFiltered', 30000, 'sparse_positions');

INSERT INTO sim_configs_v5(configId, numNodes, clusterTable, mapReduceName, windowSizeMs, filtertablename)
VALUES (37, 1000, 'self_clusters', 'geoFiltered', 30000, 'sparse_positions');

INSERT INTO sim_configs_v5(configId, numNodes, clusterTable, mapReduceName, windowSizeMs, filtertablename)
VALUES (38, 1000, 'self_clusters', 'geoFiltered', 15000, 'sparse_positions');

INSERT INTO sim_configs_v5(configId, numNodes, clusterTable, mapReduceName, windowSizeMs, filtertablename)
VALUES (39, 1000, 'self_clusters', 'geoFiltered', 10000, 'sparse_positions');

-- Self Clusters

INSERT INTO sim_configs_v5(configId, numNodes, clusterTable, mapReduceName, windowSizeMs, filtertablename)
VALUES (19, 1000, 'self_clusters', 'speedSum', 10000, 'dense_positions');

INSERT INTO sim_configs_v5(configId, numNodes, clusterTable, mapReduceName, windowSizeMs, filtertablename)
VALUES (20, 1000, 'self_clusters', 'speedSum', 15000, 'dense_positions');

INSERT INTO sim_configs_v5(configId, numNodes, clusterTable, mapReduceName, windowSizeMs, filtertablename)
VALUES (21, 1000, 'self_clusters', 'speedSum', 30000, 'dense_positions');

INSERT INTO sim_configs_v5(configId, numNodes, clusterTable, mapReduceName, windowSizeMs, filtertablename)
VALUES (22, 1000, 'self_clusters', 'speedSum', 10000, 'sparse_positions');

INSERT INTO sim_configs_v5(configId, numNodes, clusterTable, mapReduceName, windowSizeMs, filtertablename)
VALUES (23, 1000, 'self_clusters', 'speedSum', 15000, 'sparse_positions');

INSERT INTO sim_configs_v5(configId, numNodes, clusterTable, mapReduceName, windowSizeMs, filtertablename)
VALUES (24, 1000, 'self_clusters', 'speedSum', 30000, 'sparse_positions');


-- Speed Sum 2 --
insert into sim_configs_v5(configid, clustertable, numnodes, windowsizems, maxiterations, enabled, mapreducename, filtertablename, description) select (100 - configid), clustertable, numnodes, windowsizems, maxiterations, enabled, 'speedSum2', filtertablename, description from sim_configs_v5 where mapreducename='speedSum';

-- Identity --
INSERT INTO sim_configs_v5(configId, numNodes, clusterTable, mapReduceName, windowSizeMs, filtertablename)
VALUES (101, 1000, 'self_clusters', 'sendAll', 10000, 'sparse_positions');

INSERT INTO sim_configs_v5(configId, numNodes, clusterTable, mapReduceName, windowSizeMs, filtertablename)
VALUES (102, 1000, 'self_clusters', 'sendAll', 15000, 'sparse_positions');

INSERT INTO sim_configs_v5(configId, numNodes, clusterTable, mapReduceName, windowSizeMs, filtertablename)
VALUES (103, 1000, 'self_clusters', 'sendAll', 30000, 'sparse_positions');

INSERT INTO sim_configs_v5(configId, numNodes, clusterTable, mapReduceName, windowSizeMs, filtertablename)
VALUES (104, 1000, 'self_clusters', 'sendAll', 10000, 'dense_positions');

INSERT INTO sim_configs_v5(configId, numNodes, clusterTable, mapReduceName, windowSizeMs, filtertablename)
VALUES (105, 1000, 'self_clusters', 'sendAll', 15000, 'dense_positions');

INSERT INTO sim_configs_v5(configId, numNodes, clusterTable, mapReduceName, windowSizeMs, filtertablename)
VALUES (106, 1000, 'self_clusters', 'sendAll', 30000, 'dense_positions');
