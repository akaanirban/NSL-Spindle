package edu.rpi.cs.nsl.spindle.vehicle.kafka_utils

import java.util.Properties

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.global
import scala.concurrent.Future
import scala.concurrent.blocking

import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.ZkConnection
import org.slf4j.LoggerFactory

import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import kafka.cluster.Broker

class KafkaAdmin(zkString: String) {
  import AdminUtils._
  private val logger = LoggerFactory.getLogger(this.getClass)

  val ZK_CONNECT_TIMEOUT_MS = 1000
  val ZK_SESSION_TIMEOUT_MS = 10000

  val THREAD_SLEEP_MS = 100

  // Code returned when there is no error
  private val KAFKA_PARTITION_NO_ERROR_CODE = 0

  private val zkClient = ZkUtils.createZkClient(zkString, ZK_SESSION_TIMEOUT_MS, ZK_CONNECT_TIMEOUT_MS)
  private val zkUtils = new ZkUtils(zkClient, new ZkConnection(zkString), isSecure = false)

  def getBrokers: Seq[Broker] = {
    zkUtils.getAllBrokersInCluster
  }

  def waitBrokers(numBrokers: Int): Future[Seq[Broker]] = {
    def checkBrokers: Seq[Broker] = {
      val brokers = getBrokers
      logger.trace(s"Brokers $brokers")
      if (brokers.size == numBrokers) {
        return brokers
      }
      Thread.sleep(THREAD_SLEEP_MS)
      return checkBrokers
    }
    implicit val ec = global
    Future {
      blocking {
        checkBrokers
      }
    }
  }

  /**
   * Create a Kafka topic
   */
  def mkTopic(topic: String, partitions: Int = 1, replicationFactor: Int = 1, topicConfig: Properties = new Properties()): Future[Unit] = {
    createTopic(zkUtils, topic, partitions, replicationFactor, topicConfig)
    assert(topicExists(zkUtils, topic), s"Failed to create topic $topic")

    def getMetadata = AdminUtils
      .fetchTopicMetadataFromZk(topic, zkUtils)
      .partitionMetadata
      .toList

    def getPartitionErrors = getMetadata
      .map(_.error.code)
      .filterNot(_ == KAFKA_PARTITION_NO_ERROR_CODE)

    def checkPartitionLeaders {
      if (getPartitionErrors.isEmpty) {
        return
      }
      Thread.sleep(THREAD_SLEEP_MS)
      checkPartitionLeaders
    }

    implicit val ec = global
    Future {
      blocking {
        checkPartitionLeaders
      }
    }
  }

  def close {
    zkUtils.close
    zkClient.close
  }
}