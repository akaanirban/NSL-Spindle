package edu.rpi.cs.nsl.spindle.vehicle.kafka

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
import edu.rpi.cs.nsl.spindle.vehicle.TestingConfiguration
import edu.rpi.cs.nsl.spindle.vehicle.Scripts

import scala.sys.process.Process
import java.util.concurrent.TimeoutException
import scala.util.Failure
import scala.util.Success
import java.util.concurrent.Executors
import java.util.concurrent.ExecutorService
import java.util.concurrent.TimeUnit
import edu.rpi.cs.nsl.spindle.vehicle.kafka.streams.StreamKVReducer
import edu.rpi.cs.nsl.spindle.vehicle.kafka.streams.StreamMapper
import edu.rpi.cs.nsl.spindle.vehicle.kafka.streams.StreamsConfigBuilder
import edu.rpi.cs.nsl.spindle.vehicle.kafka.streams.StreamReducer
import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.KafkaAdmin
import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.ProducerKafka
import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.ConsumerKafka
import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.KafkaConfig
import scala.reflect.runtime.universe._

/**
 * Serialization test class
 */
@DoNotDiscover
class TestObj(val testVal: String) extends Serializable

private[kafka] object Constants {
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
      .withAutoOffset(false) //TODO: document why auto offset is disabled (broken offset tracking in Kafka)
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

  protected def mkProducerConsumer[K: TypeTag, V: TypeTag] = {
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

  private def getStreamsConfigBuilder = StreamsConfigBuilder()
    .withServers(baseConfig.properties.getProperty("bootstrap.servers"))
    .withZk(zkString)

  def getStreamsConfig(id: String) = getStreamsConfigBuilder.withId(id)

  private val clientFactory = new ClientFactory(zkString, baseConfig, getStreamsConfigBuilder)

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
    val mapper = new StreamMapper(inTopic, outTopic, MapperFuncs.prependText, config = getStreamsConfig("testMapper").build)
    mapper.run

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

  private object ReducerType extends Enumeration {
    val KV, Full = Value
    def getName(reducerType: Value) = reducerType match {
      case KV   => "kv"
      case Full => "full"
    }
  }

  private val kv1 = "kv1"
  private val kv2 = "kv2"

  private def testReducer(reducerType: ReducerType.Value, expectedSubstring: String) = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val reducerName = ReducerType.getName(reducerType)
    val Seq(inTopic, outTopic) = mkTopics(s"$reducerName-reducer").map { topic =>
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
    Seq(kv1, kv2).foreach(launchProducer)

    def reducerFunc(a: TestObj, b: TestObj): TestObj = {
      new TestObj(a.testVal + b.testVal)
    }

    val reducer = reducerType match {
      case ReducerType.KV   => clientFactory.mkKvReducer[TestObj, TestObj](inTopic, outTopic, reduceFunc = reducerFunc, "testKVReducer")
      case ReducerType.Full => clientFactory.mkReducer[TestObj, TestObj](inTopic, outTopic, reduceFunc=reducerFunc, "testFullReducer")
      case _                => throw new RuntimeException(s"Unrecognized reducer type $reducerName")
    }

    reducer.run

    val consumer = new ConsumerKafka[TestObj, TestObj](consumerConfig)

    def seekSubstring(messages: Iterable[(TestObj, TestObj)]): (TestObj, TestObj) = {
      val matchOpt = messages
        .filter {
          case (_, value) =>
            value.testVal.contains(expectedSubstring)
        }
        .lastOption
      matchOpt match {
        case Some(message) => message
        case None          => seekSubstring(consumer.getMessages)
      }
    }

    val outMessageFuture = subscribeAtLeastOnce(outTopic, consumer).map(msg => seekSubstring(List(msg)))

    val waitTime = 10 minutes //TODO: get from config
    val (recvdKey, recvdValue) = Await.result(outMessageFuture, waitTime)

    logger.info(s"Got message ${recvdValue.testVal}")
    (pool, consumer, reducer)
  }

  private def shutdownPool(pool: ExecutorService) {
    pool.shutdown
    pool.awaitTermination(10, TimeUnit.SECONDS)
  }

  def testKVReducer {
    logger.info("Testing KV reducer")

    val (pool, consumer, reducer) = testReducer(ReducerType.KV, s"$kv1$kv1")
    //TODO: specify window
    consumer.close
    reducer.stopStream
    shutdownPool(pool)
  }

  def testFullReducer {
    logger.info("Testing full reducer")

    val (pool, consumer, reducer) = testReducer(ReducerType.Full, s"$kv1$kv2")
    consumer.close
    reducer.stopStream
    shutdownPool(pool)

  }
}