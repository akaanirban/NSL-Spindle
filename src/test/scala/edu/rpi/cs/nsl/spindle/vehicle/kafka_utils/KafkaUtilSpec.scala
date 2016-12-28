package edu.rpi.cs.nsl.spindle.vehicle.kafka_utils

import java.io.File
import java.util.concurrent.Executors

import scala.collection.JavaConversions._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.blocking
import scala.concurrent.duration._
import scala.sys.process._

import org.scalatest.BeforeAndAfterAll
import org.scalatest.DoNotDiscover
import org.scalatest.FlatSpec
import org.slf4j.LoggerFactory

import com.spotify.docker.client.DefaultDockerClient
import com.spotify.docker.client.DockerClient.ListContainersParam
import com.spotify.docker.client.messages.Container

import edu.rpi.cs.nsl.spindle.DockerFactory
import edu.rpi.cs.nsl.spindle.vehicle.Configuration

//TODO: move into spindle docker util suite
@DoNotDiscover
private[this] object DockerHelper {
  private val dockerClient = DockerFactory.getDocker
  private val KAFKA_TYPE = "Kafka"
  private val ZK_TYPE = "Zookeeper"
  private val SCRIPTS_DIR = "scripts"
  private val KAFKA_DOCKER_DIR = s"$SCRIPTS_DIR/kafka-docker"
  private val START_KAFKA_COMMAND = s"./start.sh"
  private val STOP_KAFKA_COMMAND = s"./stop.sh"
  private val ZK_PORT = 2181
  private val KAFKA_PORT = 9092
  val NUM_KAFKA_BROKERS = 10 //TODO: get from start script or pass as param to start script

  private def runKafkaCommand(command: String) = {
    assert(Process(command, new File(KAFKA_DOCKER_DIR), "HOSTNAME" -> Configuration.hostname).! == 0, s"Command returned non-zero: $command")
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

class KafkaUtilSpec extends FlatSpec with BeforeAndAfterAll {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val KAFKA_WAIT_TIME = 120 seconds

  private lazy val kafkaAdmin = new KafkaAdmin(s"${Configuration.hostname}:2181")

  protected def kafkaConfig: KafkaConfig = {
    val servers = DockerHelper.getPorts.kafkaPorts
      .map(a => s"${Configuration.hostname}:$a")
      .reduceOption((a, b) => s"$a,$b") match {
        case Some(servers) => servers
        case None          => throw new RuntimeException("No kafka servers found")
      }
    KafkaConfig()
      .withServers(servers)
  }

  protected def producerConfig = {
    kafkaConfig.withProducerDefaults
  }

  protected def consumerConfig = {
    kafkaConfig.withConsumerDefaults.withConsumerGroup(java.util.UUID.randomUUID.toString)
  }

  protected def mkTopic: String = {
    s"test-topic-${java.util.UUID.randomUUID.toString}"
  }

  override def beforeAll {
    logger.info("Resetting kafka cluster")
    DockerHelper.stopCluster
    DockerHelper.startCluster
    logger.info(s"Waiting for kafka to converge")
    Await.ready(kafkaAdmin.waitBrokers(DockerHelper.NUM_KAFKA_BROKERS), KAFKA_WAIT_TIME)
    logger.info("Done waiting")
  }

  it should "create a producer" in {
    val producer = new ProducerKafka[Array[Byte], Array[Byte]](producerConfig)
    producer.close
  }

  private def waitMessage[K, V](consumer: ConsumerKafka[K, V]): Future[(K, V)] = {
    val executor = Executors.newSingleThreadExecutor()
    implicit val executionContext = ExecutionContext.fromExecutorService(executor)
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

  it should "send an object without crashing" in {
    val testTopic = mkTopic
    val key = new TestObj("test key")
    val value = new TestObj("test value")

    logger.debug(s"Creating topic $testTopic")
    Await.ready(kafkaAdmin.mkTopic(testTopic), KAFKA_WAIT_TIME)

    val producer = new ProducerKafka[TestObj, TestObj](producerConfig)
    val consumer = new ConsumerKafka[TestObj, TestObj](consumerConfig)

    // Consume
    consumer.subscribeAtLeastOnce(testTopic)
    val messageFuture = waitMessage(consumer)

    // Produce
    logger.info(s"Sending test message to $testTopic")
    while (Await.result(producer.send(testTopic, key, value), KAFKA_WAIT_TIME).succeeded == false) {
      logger.debug("Message send failed")
    }
    producer.flush
    logger.info("Message sent")
    producer.close

    // Wait for consumer to get message
    logger.info(s"Waiting for topic $testTopic")
    val (keyRecvd, valueRecvd) = Await.result(messageFuture, KAFKA_WAIT_TIME)
    assert(keyRecvd.testVal == key.testVal, s"$keyRecvd != $key")
    assert(valueRecvd.testVal == value.testVal, s"$valueRecvd != $value")
    consumer.close
  }

  ignore should "produce data from a data source" in {
    //TODO
    fail("Not implemented")
  }

  override def afterAll {
    kafkaAdmin.close
    logger.info("Shutting down kafka cluster")
    DockerHelper.stopCluster
  }
}