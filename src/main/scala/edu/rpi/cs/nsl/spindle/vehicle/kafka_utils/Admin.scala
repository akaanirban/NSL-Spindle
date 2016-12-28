package edu.rpi.cs.nsl.spindle.vehicle.kafka_utils

import java.util.Properties

import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.ZkConnection
import org.slf4j.LoggerFactory

import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import kafka.utils.ZKStringSerializer

class KafkaAdmin(zkString: String) {
  import AdminUtils._
  private val logger = LoggerFactory.getLogger(this.getClass)

  val ZK_CONNECT_TIMEOUT_MS = 1000
  val ZK_SESSION_TIMEOUT_MS = 10000

  private val zkClient = ZkUtils.createZkClient(zkString, ZK_SESSION_TIMEOUT_MS, ZK_CONNECT_TIMEOUT_MS)
  private val zkUtils = new ZkUtils(zkClient, new ZkConnection(zkString), isSecure = false)

  /**
   * Create a Kafka topic
   */
  def mkTopic(topic: String, partitions: Int = 1, replicationFactor: Int = 1, topicConfig: Properties = new Properties()) {
    createTopic(zkUtils, topic, partitions, replicationFactor, topicConfig)
    assert(topicExists(zkUtils, topic), s"Failed to create topic $topic")
  }

  def close {
    zkUtils.close
    zkClient.close
  }
}