package edu.rpi.cs.nsl.spindle.vehicle.kafka.utils

import java.util.Properties

import scala.collection.JavaConversions._
import scala.concurrent.{Await, Future, blocking}
import scala.concurrent.duration._
import org.I0Itec.zkclient.ZkConnection
import org.slf4j.LoggerFactory
import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import kafka.cluster.Broker
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.zookeeper.{WatchedEvent, Watcher}
import scala.concurrent.ExecutionContext.Implicits.global

class KafkaAdmin(zkString: String) {
  import edu.rpi.cs.nsl.spindle.vehicle.Configuration.Zookeeper.{sessionTimeoutMs, connectTimeoutMs}

  private val logger = LoggerFactory.getLogger(this.getClass)

  val THREAD_SLEEP_MS = 100

  // Code returned when there is no error
  private val KAFKA_PARTITION_NO_ERROR_CODE = 0

  println(s"Creating zkClient $zkString")
  private val zkClient = ZkUtils.createZkClient(zkString, sessionTimeoutMs, connectTimeoutMs)
  println(s"Finished creating zkClient $zkString")
  private val zkUtils = new ZkUtils(zkClient, new ZkConnection(zkString), isSecure = false)
  while(zkUtils.zkConnection.getZookeeperState == null) {
    println(s"Zookeeper not ready $zkString")
    zkUtils.zkConnection.connect(new Watcher{
      override def process(event: WatchedEvent): Unit = {
        println(s"ZK Connect event $event")
      }
    })
    Thread.sleep(1000)
  }
  assert(zkUtils
    .zkConnection
    .getZookeeperState
    .isConnected, s"Failed to connect to ZK $zkString")

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
    Future {
      blocking {
        checkBrokers
      }
    }
  }

  private val TOPIC_CHECK_TIMEOUT = 10 seconds

  /**
   * Check whether a topic with the specified name exists
   */
  def topicExists(topic: String): Boolean = {
    Await.result(Future{blocking{AdminUtils.topicExists(zkUtils, topic)}}, TOPIC_CHECK_TIMEOUT)
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
      val partitionErrors = Await.result(Future{blocking{getPartitionErrors}}, TOPIC_CHECK_TIMEOUT)
      if (partitionErrors.isEmpty == false) {
        println(s"Partition errors: $partitionErrors")
        Thread.sleep(THREAD_SLEEP_MS)
        checkPartitionLeaders
      }
    }
    Future {
      blocking {
        try {
          println(s"Making topic $topic")
          AdminUtils.createTopic(zkUtils, topic, partitions, replicationFactor, topicConfig)
          println(s"Created topic $topic")
        } catch {
          case _: TopicExistsException => logger.debug(s"Tried to create existing topic $topic")
        }
        println(s"Checking topic $topic")
        assert(topicExists(topic), s"Failed to create topic $topic")
        println(s"Checking partition leaders for $topic")
        checkPartitionLeaders
        println(s"Topic creation successful $topic")
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