package edu.rpi.cs.nsl.v2v.spark.streaming

import java.util.Properties

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

import org.I0Itec.zkclient.ZkClient
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.slf4j.LoggerFactory

import Serialization.KafkaKey
import Serialization.KeyDecoder
import Serialization.MiddlewareResults
import Serialization.ValueDecoder
import edu.rpi.cs.nsl.v2v.configuration.Configuration
import edu.rpi.cs.nsl.spindle.datatypes.Vehicle
import edu.rpi.cs.nsl.v2v.spark.streaming.dstream.NSLDStreamWrapper
import kafka.admin.AdminUtils
import kafka.utils.ZKStringSerializer
import kafka.serializer.DefaultDecoder
import edu.rpi.cs.nsl.spindle.ZKHelper
import edu.rpi.cs.nsl.spindle.datatypes.operations.Operation
import edu.rpi.cs.nsl.spindle.Configuration.Zookeeper

/**
 * Modeled after KafkaUtils
 *
 * @see [[https://github.com/apache/spark/blob/v2.0.1/external/kafka-0-8/src/main/scala/org/apache/spark/streaming/kafka/KafkaUtils.scala KafkaUtils Source]]
 *
 * @todo Investigate [[http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.streaming.util.WriteAheadLog WriteAheadLog]]
 */
object NSLUtils {
  private val logger = LoggerFactory.getLogger(NSLUtils.getClass)
  /**
   * Make DStream generator using configuration properties
   */
  private def mkGenerator(ssc: StreamingContext, config: StreamConfig): DStreamGenerator = {
    import Configuration._
    // Create Kafka Topic
    import config.topic
    val zkHelper = new ZKHelper(config.zkQuorum, topic)
    val convergeWaitMS = 300 // Time to wait for Kafka to sync with Zookeeper
    zkHelper.registerTopic
    val zkClient = new ZkClient(config.zkQuorum, Zookeeper.sessionTimeoutMS, Zookeeper.sessionTimeoutMS, ZKStringSerializer)
    AdminUtils.createTopic(zkClient, topic, Kafka.numPartitions, Kafka.replicationFactor, new Properties) //TODO: handle existing topic (i.e. crash recovery)
    Thread.sleep(convergeWaitMS) //SLEEP to allow Kafka to sync with Zookeeper //TODO: try getting metadata until available
    val topicProps = AdminUtils.fetchTopicConfig(zkClient, topic)
    val topicMetadata = AdminUtils.fetchTopicMetadataFromZk(topic, zkClient)
    zkClient.close
    logger.info(s"Created topic $topic: $topicProps\n\t${topicMetadata.errorCode}")
    sys.ShutdownHookThread {
      logger.info(s"Shutting down zookeeper helper")
      zkHelper.close
    }
    new DStreamGenerator(ssc, topic, config, zkHelper)
  }

  /**
   * VStream Configuration Object
   */
  case class StreamConfig(zkQuorum: String = Zookeeper.connectString,
                          brokers: String = Configuration.Kafka.brokers,
                          topic: String = java.util.UUID.randomUUID().toString)

  /**
   * Creates an NSL DStream Wrapper of default type
   */
  def createVStream(ssc: StreamingContext, config: StreamConfig = StreamConfig()): NSLDStreamWrapper[Vehicle] = {
    new NSLDStreamWrapper[Vehicle](mkGenerator(ssc, config))
  }

  /**
   * Creates an NSL DStream Wrapper of the specified type
   *
   * @note Should return an extension of Vehicle class
   */
  def createStream[T: TypeTag: ClassTag](ssc: StreamingContext, config: StreamConfig = StreamConfig()): NSLDStreamWrapper[T] = {
    new NSLDStreamWrapper[T](mkGenerator(ssc, config))
  }

  /**
   * Generates a Kafka DStream
   */
  class DStreamGenerator(ssc: StreamingContext, val topicName: String, config: StreamConfig, zkHelper: ZKHelper) {
    private val kafkaParams = Map[String, String]("metadata.broker.list" -> config.brokers)
    /**
     * Create Kafka DStream
     */
    def mkStream(opLog: Seq[Operation[_, _]]): DStream[(KafkaKey, MiddlewareResults)] = {
      zkHelper.registerQuery(opLog)
      logger.info(s"Connecting to DStream $topicName: $kafkaParams")
      KafkaUtils.createDirectStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder](ssc, kafkaParams, Set(topicName))
        .map { case (key, value) => (new KeyDecoder().fromBytes(key), new ValueDecoder().fromBytes(value)) } //TODO: replace DefaultDecoder with custom
    }
    /**
     * Get Kafka topic name
     */
    def getTopic: String = topicName
  }
}
