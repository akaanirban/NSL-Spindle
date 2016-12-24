package edu.rpi.cs.nsl.spindle.vehicle.data_sources.pubsub

/**
 * Interacts with Publish-Subscribe system (e.g. Kafka)
 */
trait PubSubClient[K, V] {
  type ByteArray = Array[Byte]
}