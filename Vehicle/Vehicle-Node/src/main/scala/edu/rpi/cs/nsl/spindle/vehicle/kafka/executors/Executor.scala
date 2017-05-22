package edu.rpi.cs.nsl.spindle.vehicle.kafka.executors

import java.util.concurrent.{RejectedExecutionException}
import java.util.concurrent.atomic.AtomicBoolean

import edu.rpi.cs.nsl.spindle.vehicle.Configuration
import edu.rpi.cs.nsl.spindle.vehicle.connections.KafkaConnection
import edu.rpi.cs.nsl.spindle.vehicle.data_sources.pubsub.SendResult
import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.{ConsumerKafka, KafkaConfig, ProducerKafka}
import org.slf4j.LoggerFactory

import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration.Duration
import scala.concurrent.duration._
import scala.concurrent.duration.MILLISECONDS
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

/**
  * Top level class for kafka stream transformation executors (e.g. Mappers, Reducers)
  * @param uid - unique identifier (has to be different for every executor)
  * @param sourceTopics
  * @param sinkTopics
  * @tparam ConsumerKey
  * @tparam ConsumerVal
  * @tparam ProducerKey
  * @tparam ProducerVal
  */
abstract class Executor[ConsumerKey: TypeTag: ClassTag,
ConsumerVal: TypeTag: ClassTag,
ProducerKey: TypeTag: ClassTag,
ProducerVal: TypeTag: ClassTag](uid: String,
                                sourceTopics: Set[GlobalTopic],
                                sinkTopics: Set[GlobalTopic])(implicit ec: ExecutionContext) {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val running = new AtomicBoolean(true)
  private var stoppedPromise: Promise[Boolean] = _
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

  protected def getConsumerQueryUid: Option[String] = None
  protected def getProducerQueryUid: Option[String] = None


  private def mkConsumer(connectionInfo: KafkaConnectionInfo, topics: Set[String]) = {
    Await.ready(initTopics(connectionInfo, topics), INIT_TOPIC_TIMEOUT) //TODO: use futures
    val config = KafkaConfig().withConsumerDefaults.withConsumerGroup(uid).withServers(connectionInfo.brokerString)
    val consumer: ConsumerKafka[ConsumerKey, ConsumerVal] = new ConsumerKafka[ConsumerKey, ConsumerVal](config, queryUid = getConsumerQueryUid)
    consumer.subscribe(topics)
    consumer
  }
  private def mkProducer(connectionInfo: KafkaConnectionInfo, topics: Set[String]) = {
    Await.ready(initTopics(connectionInfo, topics), INIT_TOPIC_TIMEOUT) //TODO: use futures
    val config = KafkaConfig().withProducerDefaults.withServers(connectionInfo.brokerString)
    val producer: ProducerKafka[ProducerKey, ProducerVal] = new ProducerKafka[ProducerKey, ProducerVal](config, queryUid = getProducerQueryUid)
    (producer, topics)
  }
  protected val consumers: Iterable[ConsumerKafka[ConsumerKey, ConsumerVal]] = sourceTopics
    .getBrokerMap
    .map{case(connectionInfo, topics) => mkConsumer(connectionInfo, topics)}

  logger.debug(s"Stream executor $uid created consumers $consumers")

  protected val producers: Iterable[(ProducerKafka[ProducerKey, ProducerVal], Set[String])] = sinkTopics
    .getBrokerMap
    .map{case(connectionInfo, topics) => mkProducer(connectionInfo, topics)}

  logger.debug(s"Stream executor $uid created producers $producers")

  protected def getMessages: Iterable[(ConsumerKey, ConsumerVal)] = {
    logger.trace(s"Stream executor $uid getting messages from $sourceTopics")
    consumers.toSeq.flatMap(_.getMessages)
  }

  protected def sendMessage(k: ProducerKey, v: ProducerVal): Seq[Future[SendResult]] = {
    logger.trace(s"Stream executor $uid sending ($k,$v) to $sinkTopics")
    producers.toSeq.flatMap{case (producer, topics) =>
      topics.map(producer.sendKafka(_, k,v))
    }
  }

  /**
    * Perform executor-specific transformations
    * @param messages - input messages
    * @return output messages
    */
  protected def doTransforms(messages: Iterable[(ConsumerKey, ConsumerVal)]): Iterable[(ProducerKey, ProducerVal)]

  /**
    * Get messages, apply transformation, publish results
    * @return Future for message publication
    */
  protected def getThenTransform: Future[Iterable[SendResult]] = {
    val inMessages = getMessages
    val outMessages = doTransforms(inMessages)
    logger.debug(s"$uid transformed $inMessages to $outMessages")
    Future.sequence(outMessages.flatMap{case (k,v) => sendMessage(k,v)})
  }

  private def runIter(sleepInterval: Duration): Unit = {
    try {
      if(running.get() == false || Thread.interrupted() == true) {
        logger.debug(s"Stopping $uid")
        stoppedPromise.success(true)
      } else {
        val sendAllFuture = getThenTransform
        Thread.sleep(sleepInterval.toMillis)
        if(sendAllFuture.isCompleted == false) {
          logger.warn(s"Warning: not all messages processed in time: $sendAllFuture")
        }
        runIter(sleepInterval)
      }
    } catch {
      case _: InterruptedException => running.set(false)
      case _: org.apache.kafka.common.errors.InterruptException => running.set(false)
      case _: RejectedExecutionException => running.set(false)
    }
  }

  def run(sleepInterval: Duration = Duration(Configuration.Streams.commitMs, MILLISECONDS)): Unit = {
    stoppedPromise = Promise[Boolean]()
    runIter(sleepInterval)
  }

  def runAsync(pool: ExecutionContext, sleepInterval: Duration = Duration(Configuration.Streams.commitMs, MILLISECONDS)): Unit = {
    logger.debug(s"Starting $uid")
    val that = this
    pool.execute(() => {
      that.run(sleepInterval)
    })
  }

  def stop: Future[Boolean] = {
    logger.debug(s"Stopping $uid")
    running.set(false)
    stoppedPromise.future
  }
}