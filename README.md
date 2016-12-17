# Spindle-Vehicle

Spindle softare running on vehicles.

## Design Decisions

- Send messages ever `delta/k` times
    - `delta` is Spark Streaming window time
- Vehicles send data continuously
    - User must expect to get more than one message per vehicle per batch

## Testing Environment

- Each vehicle is told what cluster it is in
- Data available to vehicle simulator node
    - Location
    - Speed
    - Beacons from other vehicles received in some interval
        - Indicates vehicles that are reachable in that interval
    - Probability of a message successfully being sent to another vehicle
        - Connectivity

# System Components

## All Vehicles

- Data source: produces sensor measurements of a single type
- Query manager: syncs derived queries with cloud, launches/kills related mappers/reducers
- Data mapper: performs a single map operation over a stream from one or more data sources
    - TODO: how are joins over data sources performed in a real-time setting?
        - <http://docs.confluent.io/3.0.0/streams/concepts.html#windowing>
- Cluster sink: forwards outputs of data mappers to Cluster Head

### Streams

- One stream per source
- One stream per cluster sink
    - Allow easy windowing over mapper outputs

## Cluster Head 

- Cluster source: receives data from vehicles and pushes into cluster head streams
- Data reducer: performs single derived reduce operation on incoming data
- Cloud sink: uploads reduce outputs to cloud middleware

- Aggregations for reductions should run in window that is `delta / k`

### Streams

- One stream per active mapper
    - Vehicles -> Cluster Source -> Mapper streams
- One stream for all reducers
    - Consumed by Cloud sink

## Simulator

- Time-stamped database of environment information (location, speed, indicators)
and reference to active map/reduce operations to carry out (i.e. function ID's or 
paths to serialized functions in remote storage such as ZK).
- All programs run fully connected
    - All cluster head streams visible/accessible
    - Can be run on one Kafka cluster
- Replace Cluster source and sink with test program
    - Simulating tens to hundreds of clusters
    - Read state from database
    - Select cluster head
    - Write to cluster head stream or drop message
- Replace cloud sink with program to log output 
    - Similar design to cluster source/sink simulator
    - Add delay for cluster head to cloud
- Modify Query manager to read from database

### (Some) Database Options

- Kafka Streams table
    - fewer technologies overall, but possibly more difficult to debug with
- MongoDB
    - Easy to use, controversial
- Postgres
    - Less flexible, excellent system
