package edu.rpi.cs.nsl.spindle.vehicle.kafka.executors

import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.TopicLookupService
import scala.concurrent.ExecutionContext
import scala.reflect.runtime.universe.TypeTag

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
                                    sinkTopics: Set[GlobalTopic])(implicit ec: ExecutionContext) extends Executor[K,V,K,V](uid, sourceTopics, sinkTopics){
  /**
    * Perform identity transformation (change nothing)
    *
    * @param messages - input messages
    * @return output messages
    */
  override protected def doTransforms(messages: Iterable[(K, V)]): Iterable[(K, V)] = messages
}

/**
  * Relays messages as byte arrays
  * @param uid
  * @param sourceTopics
  * @param sinkTopics
  * @param ec
  *
  * @todo ensure this technique doesn't strip off typetag information
  */
class ByteRelay(uid: String,
                sourceTopics: Set[GlobalTopic],
                sinkTopics: Set[GlobalTopic])(implicit ec: ExecutionContext) extends Relay[Array[Byte], Array[Byte]](uid, sourceTopics, sinkTopics)


object ByteRelay {
  def mkRelay(inTopics: Set[String], destination: KafkaConnectionInfo)(implicit ec: ExecutionContext) = {
    val sourceTopics = inTopics.map(GlobalTopic.mkLocalTopic)
    val sinkTopics = Set(GlobalTopic.mkGlobalTopic(TopicLookupService.getClusterInput, destination))
    println(s"Creating relay $sourceTopics -> $sinkTopics")
    new ByteRelay(uid = s"relay-${inTopics.toList.mkString("-")}", sourceTopics, sinkTopics)
  }
}