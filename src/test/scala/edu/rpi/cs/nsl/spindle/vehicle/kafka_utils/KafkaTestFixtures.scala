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
  protected def mkTopic(id: String = ""): String = {
    s"test-topic-$id-${java.util.UUID.randomUUID.toString}"
  }

  def testSendRecv {
    def testSendRecvInner(index: Int) {
      logger.info(s"$index - test send/recv")
      val testTopic = mkTopic(s"$index")
      val key = new TestObj("test key")
      val value = new TestObj("test value")

      logger.debug(s"[$index] Creating topic $testTopic")
      Await.ready(kafkaAdmin.mkTopic(testTopic), KAFKA_WAIT_TIME)
      Thread.sleep((10 seconds).toMillis) // Wait for kafka to converge

      val producer = new ProducerKafka[TestObj, TestObj](producerConfig)
      val consumer = new ConsumerKafka[TestObj, TestObj](consumerConfig)

      // Consume
      consumer.subscribeAtLeastOnce(testTopic)
      val messageFuture = waitMessage(consumer)

      // Produce
      logger.info(s"[$index] Sending test message to $testTopic")
      def sendMessage {
        var done = true
        try {
          done = Await.result(producer.send(testTopic, key, value), 2 minutes).succeeded
        } catch {
          case timeout: TimeoutException => {
            logger.warn(s"[$index] Send timeout")
            done = false
          }
        }
        if (done == true) {
          return
        }
        logger.warn(s"[$index] Sending failed for topic $testTopic")
        sendMessage
      }
      sendMessage

      producer.flush
      logger.info(s"[$index] Message sent")
      producer.close

      // Wait for consumer to get message
      logger.info(s"[$index] Waiting for topic $testTopic")
      try {
        val (keyRecvd, valueRecvd) = Await.result(messageFuture, KAFKA_WAIT_TIME)
        assert(keyRecvd.testVal == key.testVal, s"$keyRecvd != $key")
        assert(valueRecvd.testVal == value.testVal, s"$valueRecvd != $value")
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
