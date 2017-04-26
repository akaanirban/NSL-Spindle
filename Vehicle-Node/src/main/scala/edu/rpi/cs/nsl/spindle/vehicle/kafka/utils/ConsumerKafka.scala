package edu.rpi.cs.nsl.spindle.vehicle.kafka.utils

import java.util.Properties

import scala.collection.JavaConversions._
import scala.concurrent.blocking

import scala.language.postfixOps

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory

import edu.rpi.cs.nsl.spindle.vehicle.data_sources.pubsub.Consumer
import scala.concurrent.duration._
import edu.rpi.cs.nsl.spindle.vehicle.TypedValue

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
    Thread.sleep((10 seconds).toMillis) //WONTFIX: debug
    consumer.seekToBeginning
  }
}

/**
  * Wrapper for Kafka Consumer
  * @param config
  * @tparam K
  * @tparam V
  */
class ConsumerKafka[K, V](config: KafkaConfig) extends Consumer[K, V] {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private[utils] val kafkaConsumer = new KafkaConsumer[ByteArray, ByteArray](config.properties)

  val POLL_WAIT_MS = 1000

  private var topics: scala.collection.mutable.Set[String] = scala.collection.mutable.Set()

  private def subscribeWithMonitor(topics: Set[String], monitor: ConsumerBalanceMonitor[K, V]) {
    logger.debug(s"Subscribing to $topics")
    this.topics ++= topics
    kafkaConsumer.subscribe(topics, monitor)
  }

  def subscribe(topic: String) {
    this.subscribe(Seq(topic))
  }

  def subscribe(topics: Iterable[String]): Unit = {
    subscribeWithMonitor(topics.toSet, new ConsumerBalanceMonitor[K,V](this))
  }

  /**
   * Subscribe and start from beginning if reassigned
   */
  def subscribeAtLeastOnce(topic: String) {
    subscribeWithMonitor(Set(topic), new AtLeastOnceBalanceMonitor[K, V](this))
  }

  /**
   * Read and de-serialize messages in buffer
   */
  def getMessages: Iterable[(K, V)] = {
    logger.trace(s"Getting messages for $topics")
    val records = kafkaConsumer.poll(POLL_WAIT_MS)

    val rawData: List[(ByteArray, ByteArray)] = records
      .partitions().toList
      .map(records.records)
      .flatMap(_.toList)
      .map(record => (record.key(), record.value()))

    rawData
      // Remove canaries
      .filterNot{case(k,_) =>
        ObjectSerializer.deserialize[TypedValue[Any]](k).isCanary
      }
      .map {
        case (k, v) =>
          (ObjectSerializer.deserialize[TypedValue[K]](k).value, ObjectSerializer.deserialize[TypedValue[V]](v).value)
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