package edu.rpi.cs.nsl.spindle.vehicle.kafka.utils

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.blocking
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory
import edu.rpi.cs.nsl.spindle.vehicle.data_sources.pubsub.Producer
import edu.rpi.cs.nsl.spindle.vehicle.data_sources.pubsub.SendResult

import scala.reflect.runtime.universe._
import edu.rpi.cs.nsl.spindle.vehicle.TypedValue

import scala.reflect.ClassTag

/**
 * Kafka producer
 */
class ProducerKafka[K: TypeTag: ClassTag, V: TypeTag: ClassTag](config: KafkaConfig, queryUid: Option[String] = None) extends Producer[K, V] {
  private val logger = LoggerFactory.getLogger("ProducerKafka")
  private val kafkaProducer = new KafkaProducer[ByteArray, ByteArray](config.properties)
  private implicit val executionContext = ExecutionContext.global
  val CLOSE_WAIT_SECONDS = 10

  logger.trace(s"Created producer with config ${config.properties}")

  /**
    * Publish serialized data to kafka
    * @param topic
    * @param serKey
    * @param serVal
    * @return
    */
  def sendBytes(topic: String, serKey: Array[Byte], serVal: Array[Byte]) = {
    val producerRecord = new ProducerRecord[ByteArray, ByteArray](topic, serKey, serVal)
    logger.debug(s"Generated producer record $producerRecord")
    val jFuture = kafkaProducer.send(producerRecord)
    logger.debug(s"Got send future $jFuture")
    logger.debug(s"Topic replicas: ${kafkaProducer.partitionsFor(topic).toList.map(_.inSyncReplicas.toList)}")
    Future {
      blocking {
        try {
          SendResult(true, metadata = Some(jFuture.get))
        } catch {
          case e: Exception => SendResult(false, e.getMessage)
        }
      }
    }
  }

  /**
    * Publish a message with kafka-specific hacks
    * @param topic
    * @param key
    * @param value
    * @param isCanary
    * @return
    */
  def sendKafka(topic: String, key: K, value: V, isCanary: Boolean = false): Future[SendResult] = {
    logger.debug(s"Sending ($key, $value) to $topic")
    val serKey: ByteArray = ObjectSerializer.serialize(TypedValue[K](key, isCanary = isCanary, queryUid = queryUid))
    val serVal: ByteArray = ObjectSerializer.serialize(TypedValue[V](value, isCanary = isCanary, queryUid = queryUid))
    sendBytes(topic, serKey, serVal)
  }

  /**
    * Publish a message
    * @param topic
    * @param key
    * @param value
    * @return
    */
  override def send(topic: String, key: K, value: V): Future[SendResult] = sendKafka(topic, key, value)

  /**
    * Push all buffered messages to pub/sub servers
    */
  def flush {
    kafkaProducer.flush
  }

  /**
    * Close connection
    */
  def close {
    logger.trace(s"Closing producer metrics: ${kafkaProducer.metrics.toMap}")
    kafkaProducer.close()
  }
}

/**
 * Sends only to a single topic
 */
class SingleTopicProducerKakfa[K: TypeTag: ClassTag, V: TypeTag: ClassTag](topic: String, config: KafkaConfig) extends ProducerKafka[K, V](config) {
  private val logger = LoggerFactory.getLogger(s"Single Topic Producer $topic")
  def send(key: K, value: V): Future[SendResult] = super.send(topic, key, value)
  override def close: Unit = {
    logger.debug(s"Closing kafka producer: $topic")
    super.close
    logger.info(s"Kafka producer closed: $topic")
  }
}