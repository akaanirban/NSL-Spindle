package edu.rpi.cs.nsl.spindle.vehicle.kafka_utils

import java.io.File

import scala.collection.JavaConversions._
import scala.sys.process._
import scala.concurrent.Await
import scala.concurrent.duration._

import com.spotify.docker.client.DefaultDockerClient
import com.spotify.docker.client.DockerClient.ListContainersParam
import com.spotify.docker.client.messages.Container

import edu.rpi.cs.nsl.spindle.DockerFactory
import edu.rpi.cs.nsl.spindle.vehicle.Configuration
import org.slf4j.LoggerFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FlatSpec
import org.scalatest.DoNotDiscover

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

  private def runKafkaCommand(command: String) = {
    assert(Process(command, new File(KAFKA_DOCKER_DIR), "HOSTNAME" -> Configuration.hostname).! == 0)
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

class KafkaUtilSpec extends FlatSpec {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val KAFKA_WAIT_MS = 10000

  protected def kafkaConfig: KafkaConfig = {
    val servers = DockerHelper.getPorts.kafkaPorts
      .map(a => s"${Configuration.hostname}:$a")
      .reduce((a, b) => s"$a,$b")
    KafkaConfig()
      .withDefaults
      .withServers(servers)
      .withConsumerGroup(java.util.UUID.randomUUID.toString)
  }

  protected def mkTopic: String = {
    s"test-topic-${java.util.UUID.randomUUID.toString}"
  }

  /* override def beforeAll {
     * logger.info("Resetting kafka cluster")
     * DockerHelper.stopCluster
     * DockerHelper.startCluster
     * logger.info(s"Waiting $KAFKA_WAIT_MS ms for kafka to converge")
     * Thread.sleep(KAFKA_WAIT_MS)
     * logger.info("Done waiting")
    //TODO: restore
  }*/

  it should "create a producer" in {
    val producer = new ProducerKafka[Array[Byte], Array[Byte]](kafkaConfig)
    producer.close
  }

  it should "send an object without crashing" in {
    val testTopic = mkTopic
    val key = new TestObj("test key")
    val value = new TestObj("test value")

    val producer = new ProducerKafka[TestObj, TestObj](kafkaConfig)
    val consumer = new ConsumerKafka[TestObj, TestObj](kafkaConfig)
    // Produce
    logger.info("Sending test message")
    val sendResult = Await.result(producer.send(testTopic, key, value), 30 seconds)
    assert(sendResult.succeeded, sendResult.error)
    logger.info("Message sent")
    producer.close
    // Consume
    Await.result(consumer.subscribeWithFuture(testTopic), 20 seconds)
    logger.info("Retrieving test message")
    consumer.seekToBeginning
    logger.debug(s"Consumer info ${consumer.kafkaConsumer.assignment.toList}")
    assert(consumer.getMessages.size > 0)
    consumer.close
  }

  //TODO: test de-serialization

  ignore should "produce data from a data source" in {
    //TODO
  }

  /*override def afterAll {
    logger.info("Shutting down kafka cluster")
    //DockerHelper.stopCluster //TODO
  }*/
}