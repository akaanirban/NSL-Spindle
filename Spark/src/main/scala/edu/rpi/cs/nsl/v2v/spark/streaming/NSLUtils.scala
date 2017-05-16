package edu.rpi.cs.nsl.v2v.spark.streaming

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import org.I0Itec.zkclient.{ZkClient, ZkConnection}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.slf4j.LoggerFactory
import edu.rpi.cs.nsl.v2v.configuration.Configuration
import edu.rpi.cs.nsl.spindle.datatypes.Vehicle
import edu.rpi.cs.nsl.v2v.spark.streaming.dstream.NSLDStreamWrapper
import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import edu.rpi.cs.nsl.spindle.{QueryUidGenerator, ZKHelper}
import edu.rpi.cs.nsl.spindle.datatypes.operations.Operation
import edu.rpi.cs.nsl.spindle.Configuration.Zookeeper
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.clients.consumer.ConsumerRecord

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
  private def mkGenerator(ssc: StreamingContext, config: StreamConfig, queryUidGenerator: QueryUidGenerator): DStreamGenerator = {
    import Configuration._
    // Create Kafka Topic
    import config.topic
    val zkHelper = new ZKHelper(config.zkQuorum, topic, queryUidGenerator)
    val convergeWaitMS = 300 // Time to wait for Kafka to sync with Zookeeper
    zkHelper.registerTopic
    val zkClient = ZkUtils.createZkClient(config.zkQuorum, Zookeeper.sessionTimeoutMS, Zookeeper.sessionTimeoutMS)
    val zkUtils = new ZkUtils(zkClient, new ZkConnection(config.zkQuorum), isSecure = false)
    try {
      AdminUtils.createTopic(zkUtils, topic, Kafka.numPartitions, Kafka.replicationFactor)
    } catch {
      case _: TopicExistsException => logger.warn(s"Topic $topic exists")
    }
    Thread.sleep(convergeWaitMS) //SLEEP to allow Kafka to sync with Zookeeper //TODO: try getting metadata until available
    val topicMetadata = AdminUtils.fetchTopicMetadataFromZk(topic, zkUtils)
    zkClient.close
    logger.info(s"Created topic $topic:\n\t${topicMetadata.error().message()}")
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
  def createVStream(ssc: StreamingContext, config: StreamConfig = StreamConfig(), queryUidGenerator: QueryUidGenerator): NSLDStreamWrapper[Vehicle] = {
    new NSLDStreamWrapper[Vehicle](mkGenerator(ssc, config, queryUidGenerator))
  }

  /**
   * Creates an NSL DStream Wrapper of the specified type
   *
   * @note Should return an extension of Vehicle class
   */
  def createStream[T: TypeTag: ClassTag](ssc: StreamingContext, config: StreamConfig = StreamConfig(), queryUidGenerator: QueryUidGenerator): NSLDStreamWrapper[T] = {
    new NSLDStreamWrapper[T](mkGenerator(ssc, config, queryUidGenerator))
  }

  /**
   * Generates a Kafka DStream
   */
  class DStreamGenerator(ssc: StreamingContext,
                         val topicName: String,
                         config: StreamConfig,
                         zkHelper: ZKHelper) extends Serializable {
    private val kafkaParams = Map[String, Object]("bootstrap.servers" -> config.brokers,
                                                  "key.deserializer" -> classOf[ByteArrayDeserializer],
                                                  "value.deserializer" -> classOf[ByteArrayDeserializer],
                                                  // Set group ID to app ID
                                                  "group.id" -> ssc.sparkContext.applicationId)
    /**
     * Create Kafka DStream
     */
    def mkStream(opLog: Seq[Operation[_, _]]): DStream[ConsumerRecord[Array[Byte], Array[Byte]]] = {
      val queryUid: String = zkHelper.registerQuery(opLog)
      logger.info(s"Connecting to DStream $topicName: $kafkaParams")
      KafkaUtils.createDirectStream[Array[Byte], Array[Byte]](ssc, PreferConsistent, Subscribe[Array[Byte], Array[Byte]](Set(topicName), kafkaParams))
    }
    /**
     * Get Kafka topic name
     */
    def getTopic: String = topicName
  }
}
