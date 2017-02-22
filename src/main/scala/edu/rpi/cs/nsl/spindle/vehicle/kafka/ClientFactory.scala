package edu.rpi.cs.nsl.spindle.vehicle.kafka

import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.KafkaConfig
import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.ProducerKafka
import edu.rpi.cs.nsl.spindle.vehicle.kafka.streams.StreamReducer
import edu.rpi.cs.nsl.spindle.vehicle.kafka.streams.StreamKVReducer
import edu.rpi.cs.nsl.spindle.vehicle.kafka.streams.StreamsConfigBuilder
import edu.rpi.cs.nsl.spindle.vehicle.kafka.streams.StreamMapper
import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.SingleTopicProducerKakfa
import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.KafkaAdmin
import scala.concurrent._
import scala.concurrent.duration._
import edu.rpi.cs.nsl.spindle.vehicle.kafka.streams.StreamRelay
import org.slf4j.LoggerFactory
import scala.reflect.runtime.universe._
import org.apache.kafka.common.errors.TopicExistsException

//import edu.rpi.cs.nsl.spindle.datatypes.{ Vehicle => VehicleData }

case class ClientFactoryConfig(zkString: String,
                               kafkaBaseConfig: KafkaConfig,
                               streamsConfigBuilder: StreamsConfigBuilder,
                               initTopics: Boolean = true)

/**
 * Factor to make kafka clients for vehicles
 *
 * @todo - refactor interface to take shared config vlaues (zk string, brokers, id)
 */
class ClientFactory(zkString: String,
                    kafkaBaseConfig: KafkaConfig,
                    streamsConfigBuilder: StreamsConfigBuilder,
                    initTopics: Boolean = true) {
  def this(config: ClientFactoryConfig) {
    this(config.zkString: String,
      config.kafkaBaseConfig: KafkaConfig,
      config.streamsConfigBuilder: StreamsConfigBuilder,
      config.initTopics: Boolean)
  }
  private val logger = LoggerFactory.getLogger(this.getClass)
  private lazy val producerConfig = kafkaBaseConfig.withProducerDefaults
  private lazy val kafkaAdmin = new KafkaAdmin(zkString)
  private def buildConfig(id: String) = streamsConfigBuilder.withId(id).build
  private lazy val canaryProducer = new ProducerKafka[Any, Any](producerConfig)
  private var closed: Boolean = false
  private var closedStack: Array[StackTraceElement] = _
  private var closedId: Int = _

  private def initTopic(topic: String): Unit = {
    assert(topic != null, "Topic is null")
    if (initTopics) {
      if (kafkaAdmin.topicExists(topic) == false) {
        try {
          kafkaAdmin.mkTopic(topic) //note: blocking
        } catch {
          case e: TopicExistsException => {
            logger.info(s"Tried to create exsting topic $topic")
          }
        }
      }
      //TODO: make async
      Await.ready(canaryProducer.sendKafka(topic, None, None, isCanary = true), 30 seconds)
      assert(kafkaAdmin.topicExists(topic))
    }
  }

  def getConfig: ClientFactoryConfig = ClientFactoryConfig(zkString: String, kafkaBaseConfig: KafkaConfig, streamsConfigBuilder: StreamsConfigBuilder, initTopics: Boolean)

  private def getStartEpochOpt: Option[Long] = Some(System.currentTimeMillis())

  /**
   * Make a simple Kafka producer
   *
   */
  def mkProducer[K: TypeTag, V: TypeTag](outTopic: String): SingleTopicProducerKakfa[K, V] = {
    initTopic(outTopic)
    new SingleTopicProducerKakfa[K, V](outTopic, producerConfig)
  }
  def mkReducer[K: TypeTag, V: TypeTag](inTopic: String, outTopic: String, reduceFunc: (V, V) => V, reduceId: String): StreamReducer[K, V] = {
    initTopic(inTopic)
    initTopic(outTopic)
    new StreamReducer[K, V](inTopic, outTopic, reduceFunc, buildConfig(reduceId), this, startEpochOpt = getStartEpochOpt)
  }
  def mkKvReducer[K: TypeTag, V: TypeTag](inTopic: String, outTopic: String, reduceFunc: (V, V) => V, reduceId: String): StreamKVReducer[K, V] = {
    initTopic(inTopic)
    initTopic(outTopic)
    new StreamKVReducer[K, V](inTopic, outTopic, reduceFunc, buildConfig(reduceId), this, startEpochOpt = getStartEpochOpt)
  }
  def mkMapper[K: TypeTag, V: TypeTag, K1: TypeTag, V1: TypeTag](inTopic: String, outTopic: String, mapFunc: (K, V) => (K1, V1), mapId: String): StreamMapper[K, V, K1, V1] = {
    initTopic(inTopic)
    initTopic(outTopic)
    new StreamMapper[K, V, K1, V1](inTopic, outTopic, mapFunc, buildConfig(mapId), startEpochOpt = getStartEpochOpt)
  }

  def mkRelay(inTopics: Set[String], outTopic: String, relayId: String): StreamRelay = {
    inTopics.par.map(initTopic)
    initTopic(outTopic)
    logger.debug(s"Creating relay from $inTopics to $outTopic")
    new StreamRelay(relayId, inTopics, outTopic, buildConfig(relayId))
  }

  def close(nodeId: Int): Unit = {
    if(this.closed){
      val exception = new RuntimeException(s"Trying to use closed clientfactory: $nodeId")
      System.out.println(exception)
      exception.printStackTrace(System.out)
      System.out.println(s"Previously closed by $closedId: ${closedStack.toList.mkString("\n")}")
      throw exception
    } else {
      System.out.println("Closing client factory")
      closedStack = Thread.getAllStackTraces.get(Thread.currentThread())
    }
    closedId = nodeId
    logger.info("Closing client factory")
    kafkaAdmin.close
    canaryProducer.close
    this.closed = true
  }
}