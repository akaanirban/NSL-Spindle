package edu.rpi.cs.nsl.spindle

import com.spotify.docker.client.DefaultDockerClient
import com.spotify.docker.client.DockerClient.ListContainersParam
import com.spotify.docker.client.messages.ContainerConfig
import org.slf4j.LoggerFactory
import com.spotify.docker.client.DockerClient
import com.spotify.docker.client.messages.PortBinding
import com.spotify.docker.client.messages.HostConfig
import collection.JavaConversions._

import scala.collection.JavaConverters._

/**
 * Reference to a Docker container
 */
class ContainerRef(private val id: String, private val docker: DockerClient) {
  /**
   * Shut down the current container
   */
  def shutdown {
    docker.killContainer(id)
    docker.removeContainer(id)
  }

  /**
   * Get the local IP address of the container on the bridge network
   */
  def getIp = {
    docker.inspectContainer(id).networkSettings.ipAddress
  }

  /**
   * Get the host port mapped to the specified container port
   */
  def getPortMap(port: String) = {
    docker.inspectContainer(id).networkSettings.ports.get(port).last.hostPort
  }
}

/**
 * Docker test facilities
 */
class DockerFixture(imageName: String) {
  private val logger = LoggerFactory.getLogger(this.getClass)
  // Currently "works on my Mac"(TM) and should also work on Linux
  protected val docker = new DefaultDockerClient("unix:///var/run/docker.sock")

  logger.debug(s"Downloading docker image: $imageName")
  docker.pull(imageName)
  logger.debug(s"Finished downloading $imageName")

  /**
   * Create a docker container of the specified image
   */
  def launch(config: ContainerConfig.Builder): ContainerRef = {
    val container = docker.createContainer {
      config
        .image(imageName)
        .env(List("ADVERTISED_HOST=127.0.0.1"))
        .build
    }
    docker.startContainer(container.id)
    logger.info(s"Launched new container ${container.id}")
    new ContainerRef(container.id, docker)
  }
}

/**
 * Container fixutres required to run tests against Kafka and Zookeeper
 */
trait KafkaZookeeperFixture {
  protected val containerName = "wkronmiller/kafka:0.8"
  private val LAUNCH_WAIT_SECONDS = 5
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val containerManager = new DockerFixture(containerName)

  // Currently only need one container
  protected var container: ContainerRef = _

  /**
   * Start all containers required to test zookeeper and Kafka (currently only one container)
   */
  protected def startContainers {
    // Binds host ports explicitly
    val ports = Set("9092", "2181")
    val portBindings = ports
      .map { port =>
        (port -> List(PortBinding.of("0.0.0.0", port)).asJava)
      }
      .toMap
      .asJava
    logger.info(s"Creating port bindings $portBindings")
    val hostConfig = HostConfig.builder
      .portBindings(portBindings)
      //.publishAllPorts(true) // bind ports randomly
      .build
    this.container = containerManager.launch {
      ContainerConfig.builder()
        .hostConfig(hostConfig)
        .exposedPorts(ports.asJava)
    }
    Thread.sleep(1000 * LAUNCH_WAIT_SECONDS)
  }

  /**
   * Get connection IP and port
   *
   * @note Currently using port mappings to host, so IP address will always be localhost
   */
  private def getIpPort(port: String) = {
    s"127.0.0.1:${container.getPortMap(port)}"
  }

  /**
   * Get Kafka container IP and port
   */
  protected def getKafkaConnection = {
    getIpPort("9092/tcp")
  }

  /**
   * Get Zookeeper IP and port
   */
  protected def getZkConnection = {
    getIpPort("2181/tcp")
  }

  /**
   * Stop all containers
   */
  protected def stopContainers {
    container.shutdown
  }
}

/**
 * Kafka 0.10.0 container
 */
trait KafkaZookeeper10Fixture extends KafkaZookeeperFixture {
  override protected val containerName = "wkronmiller/kafka:0.10"
}
