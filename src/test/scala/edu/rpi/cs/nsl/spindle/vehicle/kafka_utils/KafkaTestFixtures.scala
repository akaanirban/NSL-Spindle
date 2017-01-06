package edu.rpi.cs.nsl.spindle.vehicle.kafka_utils

import java.io.File

import scala.collection.JavaConversions._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.blocking
import scala.concurrent.duration._

import org.scalatest.BeforeAndAfterAll
import org.scalatest.DoNotDiscover
import org.scalatest.FlatSpec
import org.slf4j.LoggerFactory

import com.spotify.docker.client.DefaultDockerClient
import com.spotify.docker.client.DockerClient.ListContainersParam
import com.spotify.docker.client.messages.Container

import edu.rpi.cs.nsl.spindle.DockerFactory
import edu.rpi.cs.nsl.spindle.vehicle.Configuration
import edu.rpi.cs.nsl.spindle.vehicle.Scripts

import scala.sys.process.Process
import java.util.concurrent.TimeoutException
import scala.util.Failure
import scala.util.Success
import java.util.concurrent.Executors
import java.util.concurrent.ExecutorService
import edu.rpi.cs.nsl.spindle.vehicle.streams.StreamMapper
import edu.rpi.cs.nsl.spindle.vehicle.streams.StreamsConfigBuilder
import java.util.concurrent.TimeUnit
import edu.rpi.cs.nsl.spindle.vehicle.streams.StreamKVReducer

//TODO: move into spindle docker util suite
@DoNotDiscover
private[kafka_utils] object DockerHelper {
  private val dockerClient = DockerFactory.getDocker
  private val KAFKA_TYPE = "Kafka"
  private val ZK_TYPE = "Zookeeper"
  private val KAFKA_DOCKER_DIR = s"${Scripts.SCRIPTS_DIR}/kafka-docker"
  private val START_KAFKA_COMMAND = s"./start.sh"
  private val STOP_KAFKA_COMMAND = s"./stop.sh"
  private val ZK_PORT = 2181
  private val KAFKA_PORT = 9092
  val NUM_KAFKA_BROKERS = 10 //TODO: get from start script or pass as param to start script

  private def runKafkaCommand(command: String) = {
    assert(Process(command, new File(KAFKA_DOCKER_DIR), "HOSTNAME" -> Configuration.dockerHost).! == 0, s"Command returned non-zero: $command")
  }

  def startCluster = runKafkaCommand(START_KAFKA_COMMAND)
  def stopCluster = runKafkaCommand(STOP_KAFKA_COMMAND)

  private def getContainers(nslType: String): List[Container] = {
    dockerClient.listContainers(ListContainersParam.withLabel("edu.rpi.cs.nsl.type", nslType)).toList
  }

  case class KafkaClusterPorts(kafkaPorts: List[Int], zkPorts: List[Int])

  def getPorts: KafkaClusterPorts = {
    def getPublicPort(privatePort: Int) = {
      (container: Container) => container.ports.toList.filter(_.getPrivatePort == privatePort).map(_.getPublicPort).last
    }
    val kafkaPorts = getContainers(KAFKA_TYPE).map(getPublicPort(KAFKA_PORT))
    val zkPorts = getContainers(ZK_TYPE).map(getPublicPort(ZK_PORT))
    KafkaClusterPorts(kafkaPorts, zkPorts)
  }
}

/**
 * Serialization test class
 */
@DoNotDiscover
class TestObj(val testVal: String) extends Serializable

private[kafka_utils] object Constants {
  val KAFKA_WAIT_TIME = 10 minutes
  val KAFKA_DEFAULT_PORT = 9092
}

@DoNotDiscover
class KafkaSharedTests(baseConfig: KafkaConfig, kafkaAdmin: KafkaAdmin) {
  import Constants._
  private val logger = LoggerFactory.getLogger(this.getClass)

  protected def producerConfig = {
    baseConfig.withProducerDefaults
  }

  protected def consumerConfig = {
    baseConfig
      .withConsumerDefaults
      .withConsumerGroup(java.util.UUID.randomUUID.toString)
      .withAutoOffset(false) //TODO: document why auto offset is disabled
  }

  /**
   * Construct a producer
   */
  def mkProducer {
    val producer = new ProducerKafka[Array[Byte], Array[Byte]](producerConfig)
    producer.close
  }

  /**
   * Wait for a consumer to get a message, then return it
   */
  private def waitMessage[K, V](consumer: ConsumerKafka[K, V]): Future[(K, V)] = {
    implicit val ec = ExecutionContext.global
    def pollMessages: (K, V) = {
      val messages = consumer.getMessages
      if (messages.size > 0) {
        return messages.last
      }
      Thread.sleep(100)
      pollMessages
    }
    Future {
      blocking {
        pollMessages
      }
    }
  }

  /**
   * Explicitly create a new topic whose name is a UUID
   */
  protected def mkTopicName(id: String = ""): String = {
    s"test-topic-$id-${java.util.UUID.randomUUID.toString}"
  }

  /**
   * Try to send message at least once
   */
  private def sendMessage[K, V](producer: ProducerKafka[K, V], topic: String, key: K, value: V) {
    var done = true
    try {
      done = Await.result(producer.send(topic, key, value), 2 minutes).succeeded
    } catch {
      case timeout: TimeoutException => {
        logger.warn(s"Send timeout")
        done = false
      }
    }
    if (done == true) {
      producer.flush
      logger.info(s"Message sent on $topic")
      return
    }
    logger.warn(s"Sending failed for topic $topic")
    sendMessage(producer, topic, key, value)
  }

  protected def subscribeAtLeastOnce[K, V](topic: String, consumer: ConsumerKafka[K, V]) = {
    consumer.subscribeAtLeastOnce(topic)
    waitMessage(consumer)
  }

  protected def mkTopicSync(topic: String) = {
    logger.debug(s"Creating topic $topic")
    Await.ready(kafkaAdmin.mkTopic(topic), KAFKA_WAIT_TIME)
    Thread.sleep((10 seconds).toMillis) // Wait for kafka to converge
  }

  protected def mkProducerConsumer[K, V] = {
    val producer = new ProducerKafka[K, V](producerConfig)
    val consumer = new ConsumerKafka[K, V](consumerConfig)
    (producer, consumer)
  }

  protected def compareKV(k1: TestObj, k2: TestObj, v1: TestObj, v2: TestObj) {
    def compare(a: TestObj, b: TestObj) {
      assert(a.testVal == b.testVal, s"${a.testVal} != ${b.testVal}")
    }
    compare(k1, k2)
    compare(v1, v2)
  }

  def testSendRecv {
    def testSendRecvInner(index: Int) {
      logger.info(s"$index - test send/recv")
      val testTopic = mkTopicName(s"$index")
      val key = new TestObj("test key")
      val value = new TestObj("test value")

      mkTopicSync(testTopic)

      val (producer, consumer) = mkProducerConsumer[TestObj, TestObj]

      // Consume
      val messageFuture = subscribeAtLeastOnce(testTopic, consumer)

      // Produce
      logger.info(s"[$index] Sending test message to $testTopic")
      sendMessage(producer, testTopic, key, value)
      producer.close

      // Wait for consumer to get message
      logger.info(s"[$index] Waiting for topic $testTopic")
      try {
        val (keyRecvd, valueRecvd) = Await.result(messageFuture, KAFKA_WAIT_TIME)
        this.compareKV(key, keyRecvd, value, valueRecvd)
      } catch {
        case e: Exception => {
          logger.error(s"$index failed: ${e.getMessage}")
          throw e
        }
      }
      consumer.close
    }
    // Run test 500 times (with some tests running in parallel)
    val failures = (0 to 100).toList.par
      .map { i =>
        try {
          Success(testSendRecvInner(i))
        } catch {
          case e: Exception => Failure(e)
        }
      }
      .filter(_.isFailure)
      .toList

    // Throw errors
    failures.foreach { _.get }
    assert(failures.length == 0, s"Failures: ${failures.length}\t$failures")
    logger.info("Done testing send/recv")
  }
}

class KafkaStreamsTestFixtures(baseConfig: KafkaConfig, kafkaAdmin: KafkaAdmin, zkString: String) extends KafkaSharedTests(baseConfig, kafkaAdmin) {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val SEND_SLEEP_TIME = 2 second
  private def getPool: ExecutorService = { Executors.newCachedThreadPool }

  private def getStreamsConfig(id: String) = StreamsConfigBuilder()
    .withId(id)
    .withServers(baseConfig.properties.getProperty("bootstrap.servers"))
    .withZk(zkString)
    .build

  private def sendContinuously[K, V](topic: String, key: K, value: V, producer: ProducerKafka[K, V])(implicit pool: ExecutorService) {
    pool.execute(new Runnable {
      def run {
        logger.info(s"Starting continuous producer for $topic")
        var running = true
        while (running) {
          producer.send(topic, key, value)
          try {
            Thread.sleep(SEND_SLEEP_TIME.toMillis)
          } catch {
            case e: InterruptedException => {
              logger.info(s"Stopping producer for $topic")
              running = false
              producer.close
            }
          }
        }
      }
    })
  }

  private object MapperFuncs {
    def prependText(k: TestObj, v: TestObj): (TestObj, TestObj) = {
      val newKey = new TestObj(s"mapped-${k.testVal}")
      val newVal = new TestObj(s"mapped-${v.testVal}")
      (newKey, newVal)
    }
  }

  private def mkTopics(prefix: String) = {
    Seq(s"$prefix-in", s"$prefix-out").map(mkTopicName)
  }

  private def mkObject(value: String) = new TestObj(value)

  def testStreamMapper {
    val Seq(inTopic, outTopic) = mkTopics("mapper")
    val key = mkObject("key")
    val value = mkObject("val-one")

    mkTopicSync(inTopic)
    mkTopicSync(outTopic)

    val (producer, consumer) = mkProducerConsumer[TestObj, TestObj]

    implicit val pool = getPool

    // Start producing
    sendContinuously(inTopic, key, value, producer)

    // Start mapper
    val mapper = new StreamMapper(inTopic, outTopic, MapperFuncs.prependText, config = getStreamsConfig("testMapper"))
    pool.execute(mapper)

    val outMessageFuture = subscribeAtLeastOnce(outTopic, consumer)

    logger.info("waiting for mapper output")
    val (recvdKey, recvdValue) = Await.result(outMessageFuture, 1 minutes) //TODO: use constant
    logger.info(s"Got message")
    consumer.close
    pool.shutdown

    val (expectedKey, expectedValue) = MapperFuncs.prependText(key, value)
    compareKV(expectedKey, recvdKey, expectedValue, recvdValue)

    logger.debug("Waiting for pool shutdown")
    pool.awaitTermination(10, TimeUnit.SECONDS)
  }

  def testKVReducer {
    logger.info("Testing KV reducer")
    val Seq(inTopic, outTopic) = mkTopics("kv-reducer").map { topic =>
      mkTopicSync(topic)
      topic
    }

    implicit val pool = getPool
    def launchProducer(objValue: String) = {
      val key = mkObject(objValue)
      val value = mkObject(objValue)
      val producer = new ProducerKafka[TestObj, TestObj](producerConfig)
      sendContinuously(inTopic, key, value, producer)
    }

    // Start producing 
    Seq("kv1", "kv2").foreach(launchProducer)

    def reducerFunc(a: TestObj, b: TestObj): TestObj = {
      new TestObj(a.testVal + b.testVal)
    }

    val reducer = new StreamKVReducer(inTopic, outTopic, reduceFunc = reducerFunc, getStreamsConfig("testKVReducer"))
    pool.execute(reducer)

    val consumer = new ConsumerKafka[TestObj, TestObj](consumerConfig)
    val outMessageFuture = subscribeAtLeastOnce(outTopic, consumer)

    val (recvdKey, recvdValue) = Await.result(outMessageFuture, 1 minutes)

    logger.info(s"Got message ${recvdValue.testVal}")

    for (i <- (0 to 10)) {
      consumer.getMessages
        .map { case (k, v) => s"${k.testVal} -> ${v.testVal}" }
        .foreach(println)
      Thread.sleep(1000)
    }
    //TODO: more sophisticated tests
    //TODO: full reduce
    //TODO: specify window
    consumer.close
    pool.shutdown

    pool.awaitTermination(10, TimeUnit.SECONDS)
  }
}