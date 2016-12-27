package edu.rpi.cs.nsl.spindle.vehicle.kafka_utils

import edu.rpi.cs.nsl.spindle.vehicle.data_sources.pubsub.Consumer
import org.slf4j.LoggerFactory
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.Properties
import scala.collection.JavaConversions._
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition
import scala.concurrent.Promise
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import java.util.concurrent.Executors

class ConsumerBalanceMonitor extends ConsumerRebalanceListener {
  type PartitionCollection = java.util.Collection[TopicPartition]

  private val logger = LoggerFactory.getLogger(this.getClass)
  
  logger.debug("Consumer balance monitor created")

  def onPartitionsRevoked(partitions: PartitionCollection) {
    logger.debug(s"Revoked partitions $partitions")
  }
  def onPartitionsAssigned(partitions: PartitionCollection) {
    logger.debug(s"Assigned partitions $partitions")
  }
}

class PartitionAddWatcher(addPromise: Promise[Iterable[TopicPartition]]) extends ConsumerBalanceMonitor {
  override def onPartitionsAssigned(partitions: PartitionCollection) {
    super.onPartitionsAssigned(partitions)
    addPromise.success(partitions)
  }
}

class ConsumerKafka[K, V](config: KafkaConfig) extends Consumer[K, V] {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private[kafka_utils] val kafkaConsumer = new KafkaConsumer[ByteArray, ByteArray](config.properties)

  val POLL_WAIT_MS = 1000

  def subscribe(topic: String) {
    kafkaConsumer.subscribe(List(topic))
  }

  def subscribeWithFuture(topic: String): Future[Iterable[String]] = {
    logger.debug(s"Subscribing with future $topic")
    val promise = Promise[Iterable[TopicPartition]]()
    val listener = new PartitionAddWatcher(promise)
    implicit val ec = new ExecutionContext {
      val thread = Executors.newSingleThreadExecutor
      def execute(runnable: Runnable) {
        logger.debug(s"Executing runnable ${runnable.getClass}")
        thread.submit(runnable)
      }
      def reportFailure(throwable: Throwable) {
        System.err.println(throwable)
      }
    }
    // Subscribe
    kafkaConsumer.subscribe(List(topic), listener)
    promise.future.map(_.map(_.topic))
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