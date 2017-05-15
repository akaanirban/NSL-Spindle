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
import scala.reflect.runtime.universe.TypeTag

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

object ConsumerKafka {
  def isCanary(typedMessage: Array[Byte]): Boolean = {
    ObjectSerializer.deserialize[TypedValue[Any]](typedMessage).isCanary
  }
}

/**
  * Wrapper for Kafka Consumer
  * @param config
  * @tparam K
  * @tparam V
  */
class ConsumerKafka[K: TypeTag, V: TypeTag](config: KafkaConfig, queryUid: Option[String] = None) extends Consumer[K, V] {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private[utils] val kafkaConsumer = new KafkaConsumer[ByteArray, ByteArray](config.properties)

  val POLL_WAIT_MS = 1000

  private var topics: scala.collection.mutable.Set[String] = scala.collection.mutable.Set()

  private def subscribeWithMonitor(topics: Set[String], monitor: ConsumerBalanceMonitor[K, V]) {
    logger.debug(s"Subscribing to $topics")
    this.topics ++= topics
    kafkaConsumer.subscribe(topics, monitor)
  }

  /**
    * Listen for messages on specified topic
    * @param topic
    */
  def subscribe(topic: String) {
    this.subscribe(Seq(topic))
  }

  /**
    * Listen for messages on all listed topics
    * @param topics
    */
  def subscribe(topics: Iterable[String]): Unit = {
    subscribeWithMonitor(topics.toSet, new ConsumerBalanceMonitor[K,V](this))
  }

  def getRawMessages: List[(ByteArray, ByteArray)] = {
    logger.trace(s"Getting messages for $topics")
    val records = kafkaConsumer.poll(POLL_WAIT_MS)
    println(s"Consumer on $topics got messages ${records.toList}")
    records
      .partitions().toList
      .map(records.records)
      .flatMap(_.toList)
      .map(record => (record.key(), record.value()))
  }

  /**
    * Read and de-serialize messages in buffer
    */
  def getMessages: Iterable[(K, V)] = {
    getRawMessages
      // Remove canaries
      .filterNot{case(k,_) =>
        ConsumerKafka.isCanary(k)
      }
      // Remove messages for other queries
      .filter{case (kBytes,vBytes) =>
        queryUid match {
          case None => true
          case Some(queryId) => {
            val k = ObjectSerializer.deserialize[TypedValue[Any]](kBytes)
            val v = ObjectSerializer.deserialize[TypedValue[Any]](vBytes)
            assert(k.queryUid == v.queryUid, s"Key/value query uid mismatch ${k.queryUid} != ${v.queryUid}")
            queryId == k.queryUid
          }
        }
      }
      .map {
        case (k, v) =>
          (ObjectSerializer.deserialize[TypedValue[K]](k).value, ObjectSerializer.deserialize[TypedValue[V]](v).value)
      }
  }

  def close {
    kafkaConsumer.close
  }
}