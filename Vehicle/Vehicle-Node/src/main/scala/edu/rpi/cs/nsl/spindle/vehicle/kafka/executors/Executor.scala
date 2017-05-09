package edu.rpi.cs.nsl.spindle.vehicle.kafka.executors

import java.util.concurrent.ExecutorService
import java.util.concurrent.atomic.AtomicBoolean

import edu.rpi.cs.nsl.spindle.vehicle.Configuration
import edu.rpi.cs.nsl.spindle.vehicle.connections.KafkaConnection
import edu.rpi.cs.nsl.spindle.vehicle.data_sources.pubsub.SendResult
import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.{ConsumerKafka, KafkaConfig, ProducerKafka}

import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration.Duration
import scala.concurrent.duration.MILLISECONDS
import scala.reflect.runtime.universe.TypeTag

/**
  * Top level class for kafka stream transformation executors (e.g. Mappers, Reducers)
  * @param uid
  * @param sourceTopics
  * @param sinkTopics
  * @tparam ConsumerKey
  * @tparam ConsumerVal
  * @tparam ProducerKey
  * @tparam ProducerVal
  */
abstract class Executor[ConsumerKey: TypeTag, ConsumerVal: TypeTag, ProducerKey: TypeTag, ProducerVal: TypeTag](uid: String,
                                                                                                       sourceTopics: Set[GlobalTopic],
                                                                                                       sinkTopics: Set[GlobalTopic])(implicit ec: ExecutionContext) {
  private val running = new AtomicBoolean(true)
  private var stoppedPromise: Promise[Boolean] = _
  private implicit class GlobalTopicSet(globalTopicSet: Set[GlobalTopic]) {
    def getBrokerMap: Map[KafkaConnectionInfo, Set[String]] = {
      //TODO: verify this logic
      globalTopicSet.map(_.toTuple).groupBy(_._1).map{case(k,kvs) => (k, kvs.map(_._2))}
    }
  }

  private def initTopics(connectionInfo: KafkaConnectionInfo, topics: Set[String]): Future[Unit] = {
    val kafkaConnection = new KafkaConnection(connectionInfo)
    val admin = kafkaConnection.getAdmin
    Future.sequence(topics.toSeq.map(admin.mkTopic(_))).map(_ => Unit)
  }

  private def mkConsumer(connectionInfo: KafkaConnectionInfo, topics: Set[String]) = {
    println(s"Stream executor $uid initializing topics $topics")
    Await.ready(initTopics(connectionInfo, topics), Duration.Inf) //TODO: use futures
    println(s"Stream executor $uid initialized topics $topics")
    val config = KafkaConfig().withConsumerDefaults.withConsumerGroup(uid).withServers(connectionInfo.brokerString)
    val consumer: ConsumerKafka[ConsumerKey, ConsumerVal] = new ConsumerKafka[ConsumerKey, ConsumerVal](config)
    consumer.subscribe(topics)
    consumer
  }
  private def mkProducer(connectionInfo: KafkaConnectionInfo, topics: Set[String]) = {
    val config = KafkaConfig().withProducerDefaults.withServers(connectionInfo.brokerString)
    val producer: ProducerKafka[ProducerKey, ProducerVal] = new ProducerKafka(config)
    (producer, topics)
  }
  private val consumers: Iterable[ConsumerKafka[ConsumerKey, ConsumerVal]] = sourceTopics
    .getBrokerMap
    .map{case(connectionInfo, topics) => mkConsumer(connectionInfo, topics)}

  println(s"Stream executor $uid created consumers $consumers")

  private val producers: Iterable[(ProducerKafka[ProducerKey, ProducerVal], Set[String])] = sinkTopics
    .getBrokerMap
    .map{case(connectionInfo, topics) => mkProducer(connectionInfo, topics)}

  println(s"Stream executor $uid created producers $producers")

  protected def getMessages: Iterable[(ConsumerKey, ConsumerVal)] = {
    println(s"Stream executor $uid getting messages from $sourceTopics")
    consumers.toSeq.flatMap(_.getMessages)
  }

  protected def sendMessage(k: ProducerKey, v: ProducerVal): Seq[Future[SendResult]] = {
    println(s"Stream executor $uid sending ($k,$v) to $sinkTopics")
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


  private def runIter(sleepInterval: Duration): Unit = {
    val inMessages = getMessages
    val outMessages = doTransforms(inMessages)
    val sendAllFuture = Future.sequence(outMessages.flatMap{case (k,v) => sendMessage(k,v)})
    try {
      Thread.sleep(sleepInterval.toMillis)
      if(sendAllFuture.isCompleted == false) {
        println(s"Warning: not all messages processed in time: $outMessages - $sendAllFuture")
      }
      if(running.get() == false) {
        println(s"Stopping $uid")
        stoppedPromise.success(true)
      } else {
        runIter(sleepInterval)
      }
    } catch {
      case _: InterruptedException => Unit
    }
  }

  def run(sleepInterval: Duration = Duration(Configuration.Streams.commitMs, MILLISECONDS)): Unit = {
    stoppedPromise = Promise[Boolean]()
    runIter(sleepInterval)
  }

  def runAsync(pool: ExecutionContext, sleepInterval: Duration = Duration(Configuration.Streams.commitMs, MILLISECONDS)): Unit = {
    println(s"Starting $uid")
    val that = this
    pool.execute(() => {
      that.run(sleepInterval)
    })
  }

  def stop: Future[Boolean] = {
    println(s"Stopping $uid")
    running.set(false)
    stoppedPromise.future
  }
}