package edu.rpi.cs.nsl.spindle.vehicle.kafka.utils

import java.util.concurrent.TimeUnit

import scala.collection.JavaConversions._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.blocking
import scala.concurrent.duration._

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory

import edu.rpi.cs.nsl.spindle.vehicle.data_sources.pubsub.Producer
import edu.rpi.cs.nsl.spindle.vehicle.data_sources.pubsub.SendResult
import kafka.common.UnknownTopicOrPartitionException

import scala.reflect.runtime.universe._
import edu.rpi.cs.nsl.spindle.vehicle.TypedValue

/**
 * Kafka producer
 */
class ProducerKafka[K: TypeTag, V: TypeTag](config: KafkaConfig) extends Producer[K, V] {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val kafkaProducer = new KafkaProducer[ByteArray, ByteArray](config.properties)
  private implicit val executionContext = ExecutionContext.global
  val CLOSE_WAIT_SECONDS = 10

  logger.trace(s"Created producer with config ${config.properties}")

  override def send(topic: String, key: K, value: V): Future[SendResult] = {
    val serKey: ByteArray = ObjectSerializer.serialize(TypedValue[K](key))
    val serVal: ByteArray = ObjectSerializer.serialize(TypedValue[V](value))
    val producerRecord = new ProducerRecord[ByteArray, ByteArray](topic, serKey, serVal)
    val jFuture = kafkaProducer.send(producerRecord)
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

  def flush {
    kafkaProducer.flush
  }

  def close {
    logger.trace(s"Closing producer ${kafkaProducer.metrics.toMap}")
    kafkaProducer.close(CLOSE_WAIT_SECONDS, TimeUnit.SECONDS)
  }
}

/**
 * Sends only to a single topic
 */
class SingleTopicProducerKakfa[K: TypeTag, V: TypeTag](topic: String, config: KafkaConfig) extends ProducerKafka[K, V](config) {
  def send(key: K, value: V): Future[SendResult] = super.send(topic, key, value)
}