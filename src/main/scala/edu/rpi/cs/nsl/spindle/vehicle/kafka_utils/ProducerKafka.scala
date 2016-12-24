package edu.rpi.cs.nsl.spindle.vehicle.kafka_utils

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.blocking

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory
import edu.rpi.cs.nsl.spindle.vehicle.data_sources.pubsub.SendResult
import edu.rpi.cs.nsl.spindle.vehicle.data_sources.pubsub.Producer

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