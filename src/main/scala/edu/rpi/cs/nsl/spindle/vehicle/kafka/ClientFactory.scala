package edu.rpi.cs.nsl.spindle.vehicle.kafka

import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.KafkaConfig
import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.ProducerKafka
import edu.rpi.cs.nsl.spindle.vehicle.kafka.streams.StreamReducer
import edu.rpi.cs.nsl.spindle.vehicle.kafka.streams.StreamKVReducer
import edu.rpi.cs.nsl.spindle.vehicle.kafka.streams.StreamsConfigBuilder
import edu.rpi.cs.nsl.spindle.vehicle.kafka.streams.StreamMapper
import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.SingleTopicProducerKakfa

//import edu.rpi.cs.nsl.spindle.datatypes.{ Vehicle => VehicleData }

/**
 * Factor to make kafka clients for vehicles
 */
class ClientFactory(kafkaBaseConfig: KafkaConfig, streamsConfigBuilder: StreamsConfigBuilder) {
  private lazy val producerConfig = kafkaBaseConfig.withProducerDefaults

  private def buildConfig(id: String) = streamsConfigBuilder.withId(id).build
  /**
   * Make a simple Kafka producer
   *
   * @todo - Automatically create topic if not exists
   */
  def mkProducer[K, V](outTopic: String) = new SingleTopicProducerKakfa[K, V](outTopic, producerConfig)
  def mkReducer[K >: Null, V >: Null](inTopic: String, outTopic: String, reduceFunc: (V, V) => V, reduceId: String) = {
    new StreamReducer[K, V](inTopic, outTopic, reduceFunc, buildConfig(reduceId))
  }
  def mkKvReducer[K >: Null, V >: Null](inTopic: String, outTopic: String, reduceFunc: (V, V) => V, reduceId: String) = {
    new StreamKVReducer[K, V](inTopic, outTopic, reduceFunc, buildConfig(reduceId))
  }
  def mkMapper[K, V, K1, V1](inTopic: String, outTopic: String, mapFunc: (K, V) => (K1, V1), mapId: String) = {
    new StreamMapper[K, V, K1, V1](inTopic, outTopic, mapFunc, buildConfig(mapId))
  }
}