package edu.rpi.cs.nsl.spindle.vehicle.kafka_utils

import java.util.Properties
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import scala.util.Success
import scala.concurrent.forkjoin.ForkJoinPool
import scala.concurrent.ExecutionContext
import scala.concurrent.blocking
import scala.concurrent.Future
import org.apache.kafka.clients.producer.RecordMetadata

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

  def withDefaults: KafkaConfig = {
    this.withByteSerDe
  }
  //TODO: acks, retries, batch size, etc...
}

/**
 * Kafka producer
 */
class Producer[K, V](config: KafkaConfig) {
  type ByteArray = Array[Byte]
  private val kafkaProducer = new KafkaProducer[ByteArray, ByteArray](config.properties)
  private implicit val executionContext = ExecutionContext.global

  def send(topic: String, key: K, value: V): Future[RecordMetadata] = {
    val serKey: ByteArray = ObjectSerializer.serialize(key)
    val serVal: ByteArray = ObjectSerializer.serialize(value)
    val producerRecord = new ProducerRecord[ByteArray, ByteArray](topic, serKey, serVal)
    val jFuture = kafkaProducer.send(producerRecord)
    Future {
      blocking {
        jFuture.get
      }
    }
  }

  def close {
    kafkaProducer.close
  }
}