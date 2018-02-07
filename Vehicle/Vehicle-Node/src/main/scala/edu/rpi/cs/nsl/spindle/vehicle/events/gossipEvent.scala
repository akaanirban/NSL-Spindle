package edu.rpi.cs.nsl.spindle.vehicle.events

import edu.rpi.cs.nsl.spindle.vehicle.Types.Timestamp
import edu.rpi.cs.nsl.spindle.vehicle.connections.KafkaConnection
import edu.rpi.cs.nsl.spindle.vehicle.data_sources.pubsub.SendResult
import edu.rpi.cs.nsl.spindle.vehicle.kafka.executors.{GlobalTopic, KafkaConnectionInfo}
import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.{KafkaConfig, ProducerKafka, TopicLookupService}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration
import scala.concurrent.duration._
import scala.concurrent.duration.MILLISECONDS
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag


class GossipEvent[K:TypeTag: ClassTag, V:TypeTag: ClassTag]
                        (sinkTopics: Set[GlobalTopic],
                         produceFunc: (K, V) )(implicit ec: ExecutionContext)
extends TemporalDaemon[Unit] {
  private val logger = LoggerFactory.getLogger(this.getClass)
  protected val uid = "hey hey"

  protected val producers: Iterable[(ProducerKafka[K, V], Set[String])] = sinkTopics
    .getBrokerMap
    .map{case(connectionInfo, topics) => mkProducer(connectionInfo, topics)}

  private implicit class GlobalTopicSet(globalTopicSet: Set[GlobalTopic]) {
    def getBrokerMap: Map[KafkaConnectionInfo, Set[String]] = {
      //TODO: verify this logic
      globalTopicSet.map(_.toTuple).groupBy(_._1).map{case(k,kvs) => (k, kvs.map(_._2))}
    }
  }
  private val INIT_TOPIC_TIMEOUT = 10 seconds

  private def initTopics(connectionInfo: KafkaConnectionInfo, topics: Set[String]): Future[Unit] = {
    logger.debug(s"Stream executor $uid initializing topics $topics")
    val kafkaConnection = new KafkaConnection(connectionInfo)
    val admin = kafkaConnection.getAdmin
    Future.sequence(topics.toSeq.map(admin.mkTopic(_))).map{_ =>
      logger.debug(s"Stream executor $uid initialized topics $topics $connectionInfo")
      Unit
    }
  }
  protected def getProducerQueryUid: Option[String] = None

  private def mkProducer(connectionInfo: KafkaConnectionInfo, topics: Set[String]) = {
    Await.ready(initTopics(connectionInfo, topics), INIT_TOPIC_TIMEOUT) //TODO: use futures
    val config = KafkaConfig().withProducerDefaults.withServers(connectionInfo.brokerString)
    val producer: ProducerKafka[K, V] = new ProducerKafka[K, V](config, queryUid = getProducerQueryUid)
    (producer, topics)
  }

  protected def sendMessage(k: K, v: V): Future[Unit] = {
    logger.trace(s"Stream executor $uid sending ($k,$v) to $sinkTopics")
    producers.toSeq.flatMap{case (producer, topics) =>
      topics.map(producer.sendKafka(_, k,v))
    }
    Future.successful(())
  }

  protected def getMessage(): (K, V) = {
    produceFunc
  }

  protected def reportMessages(): Future[Unit] = {
    logger.info(s"sending message from gossip")
    val (k, v) = getMessage()
    sendMessage(k, v)
  }
  override def executeInterval(currentTime: Timestamp): Future[Unit] = reportMessages()

  override def safeShutdown: Future[Unit] = Future.successful(Unit)
}

object GossipEvent {
  def mkGossipEvent[K: TypeTag: ClassTag,
  V: TypeTag: ClassTag](produceFunc: (K, V))(implicit ec: ExecutionContext): GossipEvent[K, V] = {
    val sinkTopics = Set(TopicLookupService.getReducerOutput).map(GlobalTopic.mkLocalTopic)
    new GossipEvent[K, V](sinkTopics = sinkTopics, produceFunc = produceFunc)
  }
}
