package edu.rpi.cs.nsl.v2v.configuration

/**
 * Runtime configuration object
 */
object Configuration {
  object Kafka {
    lazy val consumerGroup = java.util.UUID.randomUUID.toString
    lazy val brokers = "127.0.0.1:9092"
    lazy val numPartitions = 1
    lazy val replicationFactor = 1
  }
}
