package edu.rpi.cs.nsl.spindle.vehicle.kafka.executors

import edu.rpi.cs.nsl.spindle.vehicle.Configuration
import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.TopicLookupService

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Duration, MILLISECONDS}
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

/**
  * Perform ReduceByKey on Kafka messages
 *
  * @param uid
  * @param sourceTopics
  * @param sinkTopics
  * @param reduceFunc
  * @tparam K
  * @tparam V
  */
class KVReducer[K:TypeTag: ClassTag, V:TypeTag: ClassTag](uid: String,
                                      sourceTopics: Set[GlobalTopic],
                                      sinkTopics: Set[GlobalTopic],
                                      reduceFunc: (V,V) => V)(implicit ec: ExecutionContext)
  extends Executor[K,V,K,V](uid, sourceTopics, sinkTopics) {
  /**
    * Perform executor-specific transformations
    *
    * @param messages - input messages
    * @return output messages
    */
  override protected def doTransforms(messages: Iterable[(K, V)]): Iterable[(K, V)] = {
    messages
      .groupBy(_._1)
      .mapValues(_.map(_._2))
      .mapValues{values =>
        values.reduce(reduceFunc)
      }
      .toSeq
  }

  override def run(sleepInterval: Duration = Duration(Configuration.Streams.reduceWindowSizeMs, MILLISECONDS)): Unit = {
    println(s"Reducer will run every $sleepInterval")
    super.run(sleepInterval)
  }
}

/**
  * Factory for Kafka KV Reducer Executors
  */
object KVReducer {
  /**
    * Create a KV Reducer for Vehicle Data
    * @param reducerId
    * @param mapperId
    * @param reduceFunc
    * @tparam K
    * @tparam V
    * @return
    */
  def mkVehicleReducer[K: TypeTag: ClassTag, V: TypeTag: ClassTag](reducerId: String, mapperId: String, reduceFunc: (V,V) => V)(implicit ec: ExecutionContext): KVReducer[K,V] = {
    // Reducer reads from clusterhead input
    val sourceTopics = Set(TopicLookupService.getClusterInput).map(GlobalTopic.mkLocalTopic)
    val sinkTopics = Set(TopicLookupService.getReducerOutput(reducerId)).map(GlobalTopic.mkLocalTopic)
    new KVReducer[K,V](uid=reducerId, sourceTopics, sinkTopics, reduceFunc)
  }
}
