-- Positions View --
DROP VIEW node_positions CASCADE;
CREATE VIEW node_positions AS
SELECT x.node, x.reading as x, y.reading as y, s.reading as speed, x.timestamp
FROM posx x, posy y, speed s
WHERE
    (x.node = y.node and y.node = s.node) and
    (x.timestamp = y.timestamp and y.timestamp = s.timestamp)
;

-- Dense Region --
DROP VIEW dense_positions;
CREATE VIEW dense_positions AS
SELECT *
FROM node_positions
WHERE x > 50000 and x < 51000 and y > 100000 and y < 102000
;

-- Sparse Region --
DROP VIEW sparse_positions;
CREATE VIEW sparse_positions AS
SELECT *
FROM node_positions
WHERE x > 40000 and x < 53000 and y > 106800 and y < 114000
;

