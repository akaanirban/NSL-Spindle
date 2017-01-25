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

//import edu.rpi.cs.nsl.spindle.datatypes.{ Vehicle => VehicleData }

/**
 * Factor to make kafka clients for vehicles
 */
class ClientFactory(zkString: String, kafkaBaseConfig: KafkaConfig, streamsConfigBuilder: StreamsConfigBuilder, initTopics: Boolean = true)(implicit ec: ExecutionContext) {
  private lazy val producerConfig = kafkaBaseConfig.withProducerDefaults
  private lazy val kafkaAdmin = new KafkaAdmin(zkString)
  private def buildConfig(id: String) = streamsConfigBuilder.withId(id).build

  private def initTopic(topic: String): Unit = {
    assert(topic != null, "Topic is null")
    if (initTopics) {
      if (kafkaAdmin.topicExists(topic) == false) {
        kafkaAdmin.mkTopic(topic) //note: blocking
        Thread.sleep(200) //TODO!!!
      }
    }
  }

  /**
   * Make a simple Kafka producer
   *
   * @todo - Automatically create topic if not exists
   */
  def mkProducer[K, V](outTopic: String): SingleTopicProducerKakfa[K, V] = {
    initTopic(outTopic)
    new SingleTopicProducerKakfa[K, V](outTopic, producerConfig)
  }
  def mkReducer[K >: Null, V >: Null](inTopic: String, outTopic: String, reduceFunc: (V, V) => V, reduceId: String): StreamReducer[K, V] = {
    initTopic(inTopic)
    initTopic(outTopic)
    new StreamReducer[K, V](inTopic, outTopic, reduceFunc, buildConfig(reduceId))
  }
  def mkKvReducer[K >: Null, V >: Null](inTopic: String, outTopic: String, reduceFunc: (V, V) => V, reduceId: String): StreamKVReducer[K, V] = {
    initTopic(inTopic)
    initTopic(outTopic)
    new StreamKVReducer[K, V](inTopic, outTopic, reduceFunc, buildConfig(reduceId))
  }
  def mkMapper[K, V, K1, V1](inTopic: String, outTopic: String, mapFunc: (K, V) => (K1, V1), mapId: String): StreamMapper[K, V, K1, V1] = {
    initTopic(inTopic)
    initTopic(outTopic)
    new StreamMapper[K, V, K1, V1](inTopic, outTopic, mapFunc, buildConfig(mapId))
  }
}