package edu.rpi.cs.nsl.spindle.vehicle.kafka_utils

import java.util.Properties

/**
 * Wrapper for Kafka producer and consumer configs
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
    val byteDe = "org.apache.kafka.common.serialization.ByteArrayDeserializer"
    val serializerSet = List("key.serializer", "value.serializer")
      .foldLeft(this) { (config, key) => config.copyWithChange(_.put(key, byteSer)) }
    List("key.deserializer", "value.deserializer")
      .foldLeft(serializerSet) { (config, key) => config.copyWithChange(_.put(key, byteDe)) }
  }

  def withAutoTopics: KafkaConfig = {
    this.copyWithChange(_.put("auto.create.topics.enable", "true"))
  }
  
  def withAutoOffset: KafkaConfig = {
    this.copyWithChange(_.put("enable.auto.commit", "true"))
  }

  def withDefaults: KafkaConfig = {
    this.withByteSerDe
      .withAutoTopics
      .withAutoOffset
  }
  //TODO: acks, retries, batch size, etc...

  /**
   * Consumer Information
   */

  // Consumer group Id
  def withConsumerGroup(groupId: String): KafkaConfig = {
    this.copyWithChange(_.put("group.id", groupId))
  }
}