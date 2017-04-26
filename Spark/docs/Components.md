# High-Level Components

- Spark Jobs
- Cloud Middleware
- Cluster Heads
- Individual Vehicles

## Cloud Middleware

- Query Database: Zookeeper
- Middleware-to-Spark Message Bus: Kafka
  - One topic per spark job
- Query Splitter: find overlapping queries, split into non-overlapping jobs
- Query Diffuser: diffuse split jobs to vehicles/cluster heads
- Vehicle-to-middleware Message Bus: Kafka?

## Cluster Heads

- Incoming data bus: Kafka
- Reducer manager: determine what reduce operations must occur on incoming data prior to transmission to Cloud Middleware
- Reducers: perform data aggregation, write to outgoing data queue
- Publisher: batches and transmits reducer output to cloud middleware
