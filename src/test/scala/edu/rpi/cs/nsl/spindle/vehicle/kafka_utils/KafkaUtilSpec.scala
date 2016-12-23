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
import edu.rpi.cs.nsl.spindle.vehicle.NSLSpec
import edu.rpi.cs.nsl.spindle.vehicle.Configuration
import org.slf4j.LoggerFactory

//TODO: move into spindle docker util suite
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

  def getAddrs = {
    getContainers(KAFKA_TYPE)
      .map(_.id)
      .map(dockerClient.inspectContainer)
      .map(_.networkSettings.networks.values.last.ipAddress)
  }

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
class TestObj(val testVal: String) extends Serializable

class KafkaUtilSpec extends NSLSpec {
  private val logger = LoggerFactory.getLogger(this.getClass)

  protected def kafkaConfig: KafkaConfig = {
    val servers = DockerHelper.getPorts.kafkaPorts
      .map(a => s"${Configuration.hostname}:$a")
      .reduce((a, b) => s"$a,$b")
    KafkaConfig().withDefaults.withServers(servers)
  }
  before {
    DockerHelper.stopCluster
    DockerHelper.startCluster
  }

  it should "create a producer" in {
    val producer = new Producer[Array[Byte], Array[Byte]](kafkaConfig)
    producer.close
  }

  it should "send an object without crashing" in {
    val key = new TestObj("test key")
    val value = new TestObj("test value")

    val producer = new Producer[TestObj, TestObj](kafkaConfig)
    logger.info("Sending test message")
    logger.debug(s"${Await.result(producer.send("test-topic", key, value), 30 seconds)}")
    logger.info("Message sent")
    producer.close
  }

  //TODO: test de-serialization

  ignore should "get container addresses" in {
    System.err.println(DockerHelper.getAddrs)
  }

  after {
    DockerHelper.stopCluster
  }
}