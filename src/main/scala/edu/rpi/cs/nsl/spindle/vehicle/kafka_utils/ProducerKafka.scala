package edu.rpi.cs.nsl.spindle.vehicle.kafka_utils

import java.util.Properties

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.blocking

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory
import javassist.bytecode.ByteArray
import scala.reflect.ClassTag

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

case class SendResult(succeeded: Boolean, error: String = "")

/**
 * Abstract class for data stream producer
 */
abstract class Producer[K, V] {
  type ByteArray = Array[Byte]
  def send(streamName: String, key: K, value: V): Future[SendResult]
}

/**
 * Kafka producer
 */
class ProducerKafka[K, V](config: KafkaConfig) extends Producer[K, V] {

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val kafkaProducer = new KafkaProducer[ByteArray, ByteArray](config.properties)
  private implicit val executionContext = ExecutionContext.global

  logger.debug(s"Created producer with config ${config.properties}")

  override def send(topic: String, key: K, value: V): Future[SendResult] = {
    val serKey: ByteArray = ObjectSerializer.serialize(key)
    val serVal: ByteArray = ObjectSerializer.serialize(value)
    val producerRecord = new ProducerRecord[ByteArray, ByteArray](topic, serKey, serVal)
    val jFuture = kafkaProducer.send(producerRecord)
    Future {
      blocking {
        try {
          jFuture.get
          SendResult(true)
        } catch {
          case e: Exception => SendResult(false, e.getMessage)
        }

      }
    }
  }

  def close {
    kafkaProducer.close
  }
}