package edu.rpi.cs.nsl.spindle.vehicle.kafka.executors

import edu.rpi.cs.nsl.spindle.vehicle.data_sources.pubsub.SendResult
import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.{MessageLogger, TopicLookupService}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

/**
  * Relays messages from one set of servers/topics to another
  * @param uid
  * @param sourceTopics
  * @param sinkTopics
  * @tparam K
  * @tparam V
  */
class Relay[K: TypeTag: ClassTag, V: TypeTag: ClassTag](uid: String,
                                    sourceTopics: Set[GlobalTopic],
                                    sinkTopics: Set[GlobalTopic])(implicit ec: ExecutionContext)
  extends Executor[K,V,K,V](uid, sourceTopics, sinkTopics){
  /**
    * Perform identity transformation (change nothing)
    *
    * @param messages - input messages
    * @return output messages
    */
  override protected def doTransforms(messages: Iterable[(K, V)]): Iterable[(K, V)] = messages
}

/**
  * Relays messages without deserializing them
  * @param uid
  * @param sourceTopics
  * @param sinkTopics
  * @param ec
  */
class ByteRelay(uid: String, sourceTopics: Set[GlobalTopic],
               sinkTopics: Set[GlobalTopic])(implicit ec: ExecutionContext)
  extends Relay[Any, Any](uid, sourceTopics, sinkTopics) {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val messageLogger: MessageLogger = MessageLogger.mkCsvLogger(uid, sourceTopics.map(_.topic), sinkTopics.map(_.topic))

  private def logBytes(numBytes: Long): Unit = {
    messageLogger.logMessageSize(numBytes)
  }

  //TODO: filter canary messages
  private def sendBytes(k: Array[Byte], v: Array[Byte]) = {
    logBytes(k.length + v.length)
    producers.toSeq.flatMap{case (producer, topics) =>
      topics.map(producer.sendBytes(_, k,v))
    }
  }
  override def getThenTransform: Future[Iterable[SendResult]] = {
    logger.trace(s"Relay $uid getting messages from $sourceTopics")
    val messages = consumers.toSeq.flatMap(_.getRawMessages)
    logger.trace(s"Relay $uid sending ${messages.toList} to $sinkTopics")
    Future.sequence(messages.flatMap{case(k,v) => sendBytes(k,v)})
  }
}

/**
  * Factory for AnyRelays
  */
object ByteRelay {
  private val logger = LoggerFactory.getLogger(this.getClass)
  def mkClusterheadRelay(inTopics: Set[String], destination: KafkaConnectionInfo)(implicit ec: ExecutionContext) = {
    val sourceTopics = inTopics.map(GlobalTopic.mkLocalTopic)
    val sinkTopics = Set(GlobalTopic.mkGlobalTopic(TopicLookupService.getClusterInput, destination))
    logger.debug(s"Creating relay $sourceTopics -> $sinkTopics")
    new ByteRelay(uid = s"middleware-relay-${inTopics.toList.mkString("-")}", sourceTopics, sinkTopics)
  }
  def mkMiddlewareRelay(implicit ec: ExecutionContext) = {
    val sourceTopic = GlobalTopic.mkLocalTopic(TopicLookupService.getReducerOutput)
    val sinkTopic = GlobalTopic.mkCloudTopic(TopicLookupService.middlewareInput)
    logger.debug(s"Creating relay $sourceTopic -> $sinkTopic")
    new ByteRelay(uid = s"cloud-relay-$sourceTopic", Set(sourceTopic), Set(sinkTopic))
  }
}



