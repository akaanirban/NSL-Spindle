package edu.rpi.cs.nsl.spindle.vehicle.simulation

import akka.actor.ActorSystem
import akka.actor.Props
import akka.pattern.ask
import edu.rpi.cs.nsl.spindle.vehicle.simulation.properties.BasicPropertyFactory
import edu.rpi.cs.nsl.spindle.vehicle.kafka.ClientFactory
import edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations.EmptyStaticTransformationFactory
import edu.rpi.cs.nsl.spindle.vehicle.kafka.streams.StreamsConfigBuilder
import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.KafkaConfig
import akka.util.Timeout

import scala.concurrent.duration._
import org.slf4j.LoggerFactory
import scala.util.Success
import scala.concurrent.Await

trait SimulationConfig {
  protected val propertyFactory = new BasicPropertyFactory
  protected val transformationStoreFactory = new EmptyStaticTransformationFactory
  protected val clientFactory = {
    import Configuration.{ zkString, kafkaServers }
    val kafkaBaseConfig = KafkaConfig()
      .withServers(kafkaServers)
    val streamsConfigBuilder = StreamsConfigBuilder()
      .withZk(zkString)
      .withServers(kafkaServers)
    new ClientFactory(zkString: String,
      kafkaBaseConfig: KafkaConfig,
      streamsConfigBuilder: StreamsConfigBuilder)
  }
}
/**
 * Driver for vehicle simulator
 */
trait Simulator extends SimulationConfig {
  private val logger = LoggerFactory.getLogger(this.getClass)
  val WORLD_TIMEOUT = 5 minutes
  protected val actorSystem: ActorSystem
  private lazy implicit val ec = actorSystem.dispatcher

  private lazy val worldProps: Props = {
    World.props(propertyFactory,
      transformationStoreFactory,
      clientFactory,
      Some(Configuration.Vehicles.maxEnabledNodes))
  }
  private lazy val world = actorSystem.actorOf(worldProps)
  private implicit val worldTimeout = Timeout(WORLD_TIMEOUT)

  protected def initWorld {
    logger.info("Initializing world")
    val reply = Await.result(world ? World.InitSimulation, Duration.Inf)
    assert(reply.isInstanceOf[World.Ready], s"Got unexpected world message $reply")
  }
  protected def runSim {
    logger.info("Starting simulation")
    val reply = Await.result(world ? World.StartSimulation(None), Duration.Inf)
    assert(reply.isInstanceOf[World.Starting])
  }
  protected def finish {
    logger.info("Waiting to finish")
    Await.result(actorSystem.whenTerminated, Duration.Inf)
    logger.info("Finished")
  }
}

object Core extends Simulator {
  protected val actorSystem = ActorSystem("SpindleSimulator")

  def main(args: Array[String]) {
    initWorld
    println("World initialized. Press ENTER to start")
    scala.io.StdIn.readLine
    runSim
    finish
  }
}