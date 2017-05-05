package edu.rpi.cs.nsl.spindle.vehicle.kafka.executors

import edu.rpi.cs.nsl.spindle.datatypes.Vehicle
import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.TopicLookupService

import scala.concurrent.ExecutionContext
import scala.reflect.runtime.universe.TypeTag

/**
  * Perform streaming map operations
  * @param uid
  * @param sourceTopics
  * @param sinkTopics
  * @param mapFunc
  * @param filterFunc
  * @tparam K
  * @tparam V
  * @tparam K1
  * @tparam V1
  */
class Mapper[K: TypeTag, V: TypeTag, K1: TypeTag, V1: TypeTag](uid: String,
                                                               sourceTopics: Set[GlobalTopic],
                                                               sinkTopics: Set[GlobalTopic],
                                                                     mapFunc: ((K, V)) => (K1, V1),
                                                                     filterFunc: (K,V) => Boolean)(implicit ec: ExecutionContext)
extends Executor[K,V,K1,V1](uid, sourceTopics, sinkTopics){
  /**
    * Do filter then map over message streams
    *
    * @param messages - input messages
    * @return output messages
    */
  override protected def doTransforms(messages: Iterable[(K, V)]): Iterable[(K1, V1)] = {
    messages
      .filter{case (k,v) => filterFunc(k,v)}
      .map(mapFunc)
  }
}

/**
  * Factory for Kafka Mapper
  */
object Mapper {
  /**
    * Make a mapper that runs on local kafka cluster
    * @param mapperId
    * @param sourceTopicNames
    * @param sinkTopicNames
    * @param mapFunc
    * @param filterFunc
    * @tparam K
    * @tparam V
    * @tparam K1
    * @tparam V1
    * @return
    */
  def mkLocalMapper[K: TypeTag, V: TypeTag, K1: TypeTag, V1: TypeTag](mapperId: String,
                                                                      sourceTopicNames: Set[String],
                                                                      sinkTopicNames: Set[String],
                                                                      mapFunc: ((K, V)) => (K1, V1),
                                                                      filterFunc: (K,V) => Boolean)(implicit ec: ExecutionContext): Mapper[K,V,K1,V1] = {
    val sourceTopics = sourceTopicNames.map(GlobalTopic.mkLocalTopic)
    val sinkTopics = sinkTopicNames.map(GlobalTopic.mkLocalTopic)
    new Mapper[K,V,K1,V1](mapperId, sourceTopics, sinkTopics, mapFunc, filterFunc)
  }

  /**
    * Create a mapper for processing vehicle sensor data
    * @param mapperId
    * @param mapFunc
    * @param filterFunc
    * @tparam K
    * @tparam V
    * @return
    */
  def mkSensorMapper[K:TypeTag, V: TypeTag](mapperId: String,
                                            mapFunc: ((Any, Vehicle)) => (K, V),
                                            filterFunc: (Any,Vehicle) => Boolean)(implicit ec: ExecutionContext):  Mapper[Any,Vehicle,K,V] = {
    mkLocalMapper[Any, Vehicle, K,V](mapperId, Set(TopicLookupService.getVehicleStatus), Set(TopicLookupService.getMapperOutput(mapperId)), mapFunc, filterFunc)
  }
}


/**
  * Relays messages from one set of servers/topics to another
  * @param uid
  * @param sourceTopics
  * @param sinkTopics
  * @tparam K
  * @tparam V
  */
class Relay[K: TypeTag, V: TypeTag](uid: String,
                                    sourceTopics: Set[GlobalTopic],
                                    sinkTopics: Set[GlobalTopic]) {
}