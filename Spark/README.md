# About

V2V Analysis with Apache Spark 2.0.1

# Requirements

- Scala 2.11.8
- Docker 1.12.x
- Sbt 0.13

# To Run Tests

From this directory, run: 
`source env.sh && sbt test`

# Scaladoc

<http://william.kronmiller.net/NSL-Spindle/latest/api/>

# Spark Checklist

[x] Decide on central/consensus state management structure

[ ] Decide whether to run Spark at cluster head: find pros/cons

# Gotchas

- Very occasionally, the Kafka container does not initialize in time for the test and the test fails with an error relating to no brokers being available
  - Re-run tests, possibly extend sleep timeout `LAUNCH_WAIT_SECONDS` in `Docker.scala`
