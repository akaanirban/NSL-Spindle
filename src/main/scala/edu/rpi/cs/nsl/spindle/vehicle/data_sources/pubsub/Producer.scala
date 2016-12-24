package edu.rpi.cs.nsl.spindle.vehicle.data_sources.pubsub

import scala.concurrent.Future

case class SendResult(succeeded: Boolean, error: String = "")

/**
 * Abstract class for data stream producer
 */
trait Producer[K, V] extends PubSubClient[K, V] {
  /**
   * Publish a reading
   */
  def send(streamName: String, key: K, value: V): Future[SendResult]
}