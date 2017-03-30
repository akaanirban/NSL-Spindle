package edu.rpi.cs.nsl.spindle.vehicle.data_sources.pubsub

/**
 * Data stream consumer
 */
trait Consumer[K, V] extends PubSubClient[K, V] {
  def subscribe(streamName: String)
  def getMessages: Iterable[(K, V)]
}