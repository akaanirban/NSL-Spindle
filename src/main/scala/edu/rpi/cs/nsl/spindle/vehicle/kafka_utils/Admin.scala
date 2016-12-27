package edu.rpi.cs.nsl.spindle.vehicle.kafka_utils

import java.util.Properties

import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.ZkConnection
import org.slf4j.LoggerFactory

import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import kafka.utils.ZKStringSerializer

class KafkaAdmin(zkString: String) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Create a Kafka topic
   */
  def mkTopic(topic: String, partitions: Int = 1, replicationFactor: Int = 1, topicConfig: Properties = new Properties()) {
    val zkClient = ZkUtils.createZkClient(zkString, 1000, 1000)
    val zkUtils = new ZkUtils(zkClient, new ZkConnection(zkString), isSecure = false)
    AdminUtils.createTopic(zkUtils, topic, partitions, replicationFactor, topicConfig)
    zkClient.close
  }
}