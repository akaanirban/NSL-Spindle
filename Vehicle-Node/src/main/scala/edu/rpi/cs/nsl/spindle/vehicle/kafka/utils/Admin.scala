package edu.rpi.cs.nsl.spindle.vehicle.kafka.utils

import java.util.Properties

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.global
import scala.concurrent.Future
import scala.concurrent.blocking
import scala.concurrent.duration._
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.ZkConnection
import org.slf4j.LoggerFactory
import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import kafka.cluster.Broker
import org.apache.kafka.common.errors.TopicExistsException


class KafkaAdmin(zkString: String) {
  import edu.rpi.cs.nsl.spindle.vehicle.Configuration.Zookeeper.{sessionTimeoutMs, connectTimeoutMs}

  private val logger = LoggerFactory.getLogger(this.getClass)


  val THREAD_SLEEP_MS = 100

  // Code returned when there is no error
  private val KAFKA_PARTITION_NO_ERROR_CODE = 0

  private val zkClient = ZkUtils.createZkClient(zkString, sessionTimeoutMs, connectTimeoutMs)
  private val zkUtils = new ZkUtils(zkClient, new ZkConnection(zkString), isSecure = false)

  def getBrokers: Seq[Broker] = {
    zkUtils.getAllBrokersInCluster
  }

  def waitBrokers(numBrokers: Int): Future[Seq[Broker]] = {
    def checkBrokers: Seq[Broker] = {
      val brokers = getBrokers
      logger.trace(s"Brokers $brokers")
      if (brokers.size == numBrokers) {
        brokers
      } else {
        Thread.sleep(THREAD_SLEEP_MS)
        checkBrokers
      }
    }
    implicit val ec = global
    Future {
      blocking {
        checkBrokers
      }
    }
  }

  /**
   * Check whether a topic with the specified name exists
   */
  def topicExists(topic: String): Boolean = {
    AdminUtils.topicExists(zkUtils, topic)
  }

  /**
   * Create a Kafka topic
   */
  def mkTopic(topic: String, partitions: Int = 1, replicationFactor: Int = 1, topicConfig: Properties = new Properties()): Future[Unit] = {


    def getMetadata = AdminUtils
      .fetchTopicMetadataFromZk(topic, zkUtils)
      .partitionMetadata
      .toList

    def getPartitionErrors = getMetadata
      .map(_.error.code)
      .filterNot(_ == KAFKA_PARTITION_NO_ERROR_CODE)

    def checkPartitionLeaders {
      if (getPartitionErrors.isEmpty == false) {
        Thread.sleep(THREAD_SLEEP_MS)
        checkPartitionLeaders
      }
    }

    implicit val ec = global
    Future {
      blocking {
        try {
          AdminUtils.createTopic(zkUtils, topic, partitions, replicationFactor, topicConfig)
        } catch {
          case _: TopicExistsException => logger.debug(s"Tried to create existing topic $topic")
        }
        assert(topicExists(topic), s"Failed to create topic $topic")
        checkPartitionLeaders
      }
    }
  }

  def wipeCluster {
    val topics = AdminUtils.fetchAllTopicConfigs(zkUtils).map(_._1)
    logger.info(s"Deleting topics: $topics")
    topics.foreach{topic =>
      try {
        AdminUtils.deleteTopic(zkUtils, topic)
      } catch {
        case _: kafka.common.TopicAlreadyMarkedForDeletionException => logger.warn(s"Topic already marked for deletion: $topic")
        case _: org.apache.kafka.common.errors.UnknownTopicOrPartitionException => logger.warn(s"Tried to delete unknown topic $topic")
      }
    }
    Thread.sleep((5 seconds).toMillis) //TODO: remove magic
  }

  def close {
    println("Kafka admin closing zk connection")
    zkUtils.close()
    println("ZK connection closed")
  }
}