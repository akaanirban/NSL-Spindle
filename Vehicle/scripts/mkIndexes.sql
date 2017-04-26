create index per_node_ts on id(node, timestamp);
create index idx_posx_ts on posx(node, timestamp);
create index idx_posy_ts on posy(node, timestamp);
create index idx_speed_ts on speed(node, timestamp);
cluster verbose posx using idx_posx_ts;
cluster verbose posy using idx_posy_ts;
cluster verbose speed using idx_speed_ts;
