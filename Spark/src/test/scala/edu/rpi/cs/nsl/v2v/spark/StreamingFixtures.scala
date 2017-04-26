package edu.rpi.cs.nsl.v2v.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.scalatest.BeforeAndAfter
import org.scalatest.FlatSpec
import org.scalatest.GivenWhenThen
import org.apache.spark.streaming.Seconds

/**
 * Spark Streaming test fixtures
 *
 * @todo: use embedded ZK and Kafka
 *
 * @see [[https://github.com/apache/kafka/blob/60ad6d727861a87fa756918a7be7547e9b1f4c3d/core/src/test/scala/unit/kafka/zk/ZooKeeperTestHarness.scala Zookeeper Test Harness]]
 *
 * @see [[https://github.com/apache/kafka/blob/41e676d29587042994a72baa5000a8861a075c8c/streams/src/test/java/org/apache/kafka/streams/integration/utils/KafkaEmbedded.java KafkaEmbedded]]
 */
trait StreamingFixtures {
  private val master = "local[*]"
  private val appName = "NSLSparkTest"
  private val batchDuration = Seconds(1)

  /**
   * Get initialized SparkContext object
   *
   * Run inside a before{} block
   */
  def getSSC: StreamingContext = {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
    new StreamingContext(conf, batchDuration)
  }
  /**
   * Clean up after test
   */
  def cleanAfter(ssc: StreamingContext) {
    ssc.stop()
  }
}