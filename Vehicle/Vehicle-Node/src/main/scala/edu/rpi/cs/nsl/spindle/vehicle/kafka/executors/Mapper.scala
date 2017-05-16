package edu.rpi.cs.nsl.spindle.vehicle.kafka.executors

import edu.rpi.cs.nsl.spindle.datatypes.Vehicle
import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.TopicLookupService

import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag
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
abstract class Mapper[K: TypeTag: ClassTag,
V: TypeTag: ClassTag,
K1: TypeTag: ClassTag,
V1: TypeTag: ClassTag](uid: String,
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
  * Perform map operations over sensor data
  * @param uid
  * @param queryUid
  * @param sourceTopics
  * @param sinkTopics
  * @param mapFunc
  * @param filterFunc
  * @param ec
  * @tparam K1
  * @tparam V1
  */
class SensorMapper[K1: TypeTag: ClassTag,
V1: TypeTag: ClassTag](uid: String,
                       queryUid: String,
                       sourceTopics: Set[GlobalTopic],
                       sinkTopics: Set[GlobalTopic],
                       mapFunc: ((Any, Vehicle)) => (K1, V1),
                       filterFunc: (Any,Vehicle) => Boolean)(implicit ec: ExecutionContext)
  extends Mapper[Any,Vehicle,K1,V1](uid: String,
    sourceTopics: Set[GlobalTopic],
    sinkTopics: Set[GlobalTopic],
    mapFunc: ((Any, Vehicle)) => (K1, V1),
    filterFunc: (Any,Vehicle) => Boolean){

  // Sensor data has no query UID tag
  override def getConsumerQueryUid: Option[String] = None
  // Tag output data
  override def getProducerQueryUid: Option[String] = Some(queryUid)
}

/**
  * Factory for Kafka Mapper
  */
object Mapper {
  /**
    * Create a mapper for processing vehicle sensor data
    * @param mapperId
    * @param mapFunc
    * @param filterFunc
    * @tparam K1
    * @tparam V1
    * @return
    */
  def mkSensorMapper[K1:TypeTag: ClassTag,
  V1: TypeTag: ClassTag](mapperId: String,
                        queryUid: String,
                        mapFunc: ((Any, Vehicle)) => (K1, V1),
                        filterFunc: (Any,Vehicle) => Boolean)(implicit ec: ExecutionContext):  Mapper[Any,Vehicle,K1,V1] = {
    val sourceTopics = Set(GlobalTopic.mkLocalTopic(TopicLookupService.getVehicleStatus))
    val sinkTopics = Set(GlobalTopic.mkLocalTopic(TopicLookupService.getMapperOutput(mapperId)))
    new SensorMapper[K1,V1](mapperId, queryUid, sourceTopics, sinkTopics, mapFunc, filterFunc)
  }
}


