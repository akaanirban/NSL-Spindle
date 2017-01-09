package edu.rpi.cs.nsl.spindle.vehicle.simulation

import java.io.File
import org.scalatest.DoNotDiscover
import com.spotify.docker.client.DockerClient.ListContainersParam
import com.spotify.docker.client.messages.Container
import scala.sys.process._
import scala.collection.JavaConversions._
import edu.rpi.cs.nsl.spindle.DockerFactory
import edu.rpi.cs.nsl.spindle.vehicle.Scripts
import edu.rpi.cs.nsl.spindle.vehicle.TestingConfiguration
import java.util.concurrent.atomic.AtomicInteger
import org.slf4j.LoggerFactory

//TODO: move into spindle docker util suite
@DoNotDiscover
private[vehicle] object DockerHelper {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val dockerClient = DockerFactory.getDocker
  private val KAFKA_TYPE = "Kafka"
  private val ZK_TYPE = "Zookeeper"
  private val KAFKA_DOCKER_DIR = s"${Scripts.SCRIPTS_DIR}/kafka-docker"
  private val START_KAFKA_COMMAND = s"./start.sh"
  private val STOP_KAFKA_COMMAND = s"./stop.sh"
  private val ZK_PORT = 2181
  private val KAFKA_PORT = 9092
  val NUM_KAFKA_BROKERS = 10 //TODO: get from start script or pass as param to start script
  val numClients = new AtomicInteger(0)

  private def runKafkaCommand(command: String) = {
    assert(Process(command, new File(KAFKA_DOCKER_DIR), "HOSTNAME" -> TestingConfiguration.dockerHost).! == 0, s"Command returned non-zero: $command")
  }

  def startCluster = {
    logger.info(s"Number of active kafka tests: ${numClients.incrementAndGet}")
    runKafkaCommand(START_KAFKA_COMMAND)
  }
  def stopCluster = {
    if (numClients.decrementAndGet == 0) {
      runKafkaCommand(STOP_KAFKA_COMMAND)
    }
  }

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