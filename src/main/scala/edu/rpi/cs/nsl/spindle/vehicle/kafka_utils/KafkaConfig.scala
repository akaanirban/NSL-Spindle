package edu.rpi.cs.nsl.spindle.vehicle.kafka_utils

import java.util.Properties

/**
 * Wrapper for Kafka producer config
 *
 * @see [[http://kafka.apache.org/0101/javadoc/index.html?org/apache/kafka/clients/producer/KafkaProducer.html Kafka Producer Javadocs]]
 */
case class KafkaConfig(properties: Properties = new Properties()) {
  private def getPropsCopy = {
    val propsCopy = new Properties()
    propsCopy.putAll(properties)
    propsCopy
  }
  private def copyWithChange(f: Properties => Unit) = {
    val propsNext = getPropsCopy
    f(propsNext)
    KafkaConfig(propsNext)
  }
  def withServers(servers: String): KafkaConfig = {
    copyWithChange(_.put("bootstrap.servers", servers))
  }
  /**
   * Use byte array serialization
   */
  def withByteSerDe: KafkaConfig = {
    val byteSer = "org.apache.kafka.common.serialization.ByteArraySerializer"
    copyWithChange(_.put("key.serializer", byteSer))
      .copyWithChange(_.put("value.serializer", byteSer))
  }

  def withAutoTopics: KafkaConfig = {
    this.copyWithChange(_.put("auto.create.topics.enable", "true"))
  }

  def withDefaults: KafkaConfig = {
    this.withByteSerDe
      .withAutoTopics
  }
  //TODO: acks, retries, batch size, etc...
}