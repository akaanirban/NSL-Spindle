# Kafka Streams

- [Example Scala Program](https://github.com/confluentinc/examples/blob/kafka-0.10.0.0-cp-3.0.0/kafka-streams/src/main/scala/io/confluent/examples/streams/MapFunctionScalaExample.scala)
- [Confluent Documentation](http://docs.confluent.io/3.0.0/streams/concepts.html#streams-concepts-time)
- [Confluent Blog Entry Explaining Use Cases](https://www.confluent.io/blog/introducing-kafka-streams-stream-processing-made-simple/)

## Basic Information

- True stream processing (not micro-batch)
- At-least-once delivery semantics
  - Exactly-once promised in the future
  - Integral part of Kafka 0.10
  - Supports "stateful stream processing"
    - Can treat Kafka topics as database WAL
