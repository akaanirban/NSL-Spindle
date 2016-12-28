package edu.rpi.cs.nsl.spindle.vehicle.kafka_utils

import edu.rpi.cs.nsl.spindle.vehicle.data_sources.pubsub.Consumer
import org.slf4j.LoggerFactory
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.Properties
import scala.collection.JavaConversions._
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition
import scala.concurrent.blocking
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import java.util.concurrent.Executors
import scala.concurrent.Promise

class ConsumerBalanceMonitor[K, V](consumer: ConsumerKafka[K, V]) extends ConsumerRebalanceListener {
  type PartitionCollection = java.util.Collection[TopicPartition]

  private val logger = LoggerFactory.getLogger(this.getClass)

  logger.debug("Consumer balance monitor created")

  def onPartitionsRevoked(partitions: PartitionCollection) {
    logger.debug(s"Revoked partitions $partitions for consumer ${consumer.kafkaConsumer.assignment()}")
  }
  def onPartitionsAssigned(partitions: PartitionCollection) {
    logger.debug(s"Assigned partitions $partitions for consumer ${consumer.kafkaConsumer.assignment()}")
  }
}

class AtLeastOnceBalanceMonitor[K, V](consumer: ConsumerKafka[K, V]) extends ConsumerBalanceMonitor[K, V](consumer) {
  override def onPartitionsAssigned(partitions: PartitionCollection) {
    super.onPartitionsAssigned(partitions)
    consumer.seekToBeginning
  }
}

class ConsumerKafka[K, V](config: KafkaConfig) extends Consumer[K, V] {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private[kafka_utils] val kafkaConsumer = new KafkaConsumer[ByteArray, ByteArray](config.properties)

  val POLL_WAIT_MS = 1000

  private def subscribeWithMonitor(topic: String, monitor: ConsumerBalanceMonitor[K, V]) {
    logger.debug(s"Subscribing to $topic")
    kafkaConsumer.subscribe(List(topic), monitor)
  }

  def subscribe(topic: String) {
    subscribeWithMonitor(topic, new ConsumerBalanceMonitor[K, V](this))
  }

  /**
   * Subscribe and start from beginning if reassigned
   */
  def subscribeAtLeastOnce(topic: String) {
    subscribeWithMonitor(topic, new AtLeastOnceBalanceMonitor[K, V](this))
  }

  /**
   * Read and de-serialize messages in buffer
   */
  def getMessages: Iterable[(K, V)] = {
    logger.debug("Getting messages")
    val records = kafkaConsumer.poll(POLL_WAIT_MS)
    val rawData: List[(ByteArray, ByteArray)] = records.partitions
      .map(records.records)
      .map(_.toList)
      .reduceOption((a, b) => a ++ b) match {
        case Some(list) => list.map(record => (record.key, record.value))
        case None       => List()
      }
    rawData.map {
      case (k, v) =>
        (ObjectSerializer.deserialize[K](k), ObjectSerializer.deserialize[V](v))
    }
  }

  /**
   * Start reading messages from beginning of queue
   */
  def seekToBeginning {
    kafkaConsumer.seekToBeginning(kafkaConsumer.assignment)
  }

  def close {
    kafkaConsumer.close
  }
}