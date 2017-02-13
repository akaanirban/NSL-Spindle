package edu.rpi.cs.nsl.spindle.vehicle.kafka.utils

import java.util.Properties
import scala.language.implicitConversions

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

  private implicit def bool2String(bool: Boolean): String = {
    bool match {
      case false => "false"
      case true  => "true"
    }
  }

  def withServers(servers: String): KafkaConfig = {
    copyWithChange(_.put("bootstrap.servers", servers))
  }
  /**
   * Use byte array serialization
   */
  def withByteSer: KafkaConfig = {
    val byteSer = "org.apache.kafka.common.serialization.ByteArraySerializer"
    List("key.serializer", "value.serializer")
      .foldLeft(this) { (config, key) => config.copyWithChange(_.put(key, byteSer)) }
  }

  def withByteDeser: KafkaConfig = {
    val byteDe = "org.apache.kafka.common.serialization.ByteArrayDeserializer"
    List("key.deserializer", "value.deserializer")
      .foldLeft(this) { (config, key) => config.copyWithChange(_.put(key, byteDe)) }
  }

  def withRetries(retries: Int = 0): KafkaConfig = {
    this.copyWithChange(_.put("retries", retries.toString))
  }

  def withAcks(acks: String = "all"): KafkaConfig = {
    this.copyWithChange(_.put("acks", acks))
  }

  def withBlockingOnFull(block: Boolean = true): KafkaConfig = {
    this.copyWithChange(_.put("block.on.buffer.full", block: String))
  }

  def withMaxBlockMs(ms: Long = 3000): KafkaConfig = {
    this.copyWithChange(_.put("max.block.ms", ms.toString))
  }

  def withAutoOffset(autoCommit: Boolean = true): KafkaConfig = {
    this.copyWithChange(_.put("enable.auto.commit", autoCommit: String))
  }

  def withProducerDefaults: KafkaConfig = {
    this.withByteSer
      .withRetries()
      .withAcks()
        .withMaxBlockMs()
  }

  def withConsumerDefaults: KafkaConfig = {
    this.withAutoOffset()
      .withByteDeser
  }

  //TODO: batch size, etc...

  // Consumer group Id
  def withConsumerGroup(groupId: String): KafkaConfig = {
    this.copyWithChange(_.put("group.id", groupId))
  }
}