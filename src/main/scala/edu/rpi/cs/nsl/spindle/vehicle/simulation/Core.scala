package edu.rpi.cs.nsl.spindle.vehicle.simulation

import java.io.File
import java.nio.file.Files

import akka.actor.ActorSystem
import akka.actor.Props
import akka.pattern.ask
import edu.rpi.cs.nsl.spindle.vehicle.simulation.properties.BasicPropertyFactory
import edu.rpi.cs.nsl.spindle.vehicle.kafka.ClientFactory
import edu.rpi.cs.nsl.spindle.vehicle.kafka.streams.StreamsConfigBuilder
import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.KafkaConfig
import akka.util.Timeout

import scala.concurrent.duration._
import org.slf4j.LoggerFactory

import scala.util.Success
import scala.concurrent.Await
import edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations.TransformationStoreFactory
import edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations.ActiveTransformations
import edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations.GenerativeStaticTransformationFactory
import edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations.MapperFunc
import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.TopicLookupService
import edu.rpi.cs.nsl.spindle.datatypes.{Vehicle => VehicleMessage}
import edu.rpi.cs.nsl.spindle.datatypes.VehicleTypes
import edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations.KvReducerFunc
import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.KafkaAdmin

trait SimulationConfig {
  protected val propertyFactory = new BasicPropertyFactory
  protected val transformationStoreFactory: TransformationStoreFactory
  protected lazy val clientFactory = {
    import Configuration.{ zkString, kafkaServers }
    val kafkaBaseConfig = KafkaConfig()
      .withServers(kafkaServers)
    val streamsConfigBuilder = StreamsConfigBuilder()
      .withZk(zkString)
      .withServers(kafkaServers)
      .withDefaults
    new ClientFactory(zkString: String,
      kafkaBaseConfig: KafkaConfig,
      streamsConfigBuilder: StreamsConfigBuilder)
  }
}
/**
 * Driver for vehicle simulator
 */
trait Simulator extends SimulationConfig {
  private val logger = LoggerFactory.getLogger("Simulator Driver")
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

  protected def clearKafka {
    val admin = new KafkaAdmin(Configuration.zkString)
    logger.warn("Wiping kafka cluster")
    admin.wipeCluster
    logger.debug("Closing kafka admin")
    admin.close
    logger.info("Waiting for kafka to settle")
    Thread.sleep((10 seconds).toMillis)
  }

  protected def initWorld {
    logger.info("Initializing world")
    val reply = Await.result(world ? World.InitSimulation(), Duration.Inf)
    assert(reply.isInstanceOf[World.Ready], s"Got unexpected world message $reply")
  }
  protected def runSim {
    val WORLD_DONE_TIMEOUT = (10 minutes)
    logger.info("Starting simulation")
    val reply = Await.result(world.ask(World.StartSimulation(None))(WORLD_DONE_TIMEOUT), Duration.Inf)
    logger.info("World is starting")
    assert(reply.isInstanceOf[World.Starting])
  }
  private def checkFinished: Unit = {
    Await.result(world ? World.CheckDone(), Duration.Inf) match {
      case World.Finished() => {
        logger.info("Simulator has detected that world has finished")
        actorSystem.stop(world)
        logger.info("Stopped world. Terminating actor system.")
        actorSystem.terminate()
        logger.info("Requested actor system shutdown")
      }
      case World.NotFinished() => {
        Thread.sleep((1 second).toMillis)
        checkFinished
      }
      case m: Any => throw new RuntimeException(s"Unexpected message from world actor: $m")
    }
  }
  protected def finish {
    logger.info(s"Waiting to finish. Results stored in ${Configuration.simResultsDir}")
    checkFinished
    logger.info("Waiting for actor system to finish terminating")
    Await.result(actorSystem.whenTerminated, Duration.Inf)
    logger.info("Finished")
  }
}

trait SpeedSumSimulation {
  import VehicleTypes.MPH
  val baseId = "getSpeed"
  val mapperBaseId = s"$baseId-mapper"
  val reducerBaseId = s"$baseId-reducer"
  private def uuid = java.util.UUID.randomUUID.toString
  protected val transformationStoreFactory: TransformationStoreFactory = new GenerativeStaticTransformationFactory(nodeId => {
    val mapper = {
      val mapperId = s"$mapperBaseId-$nodeId-$uuid"
      val inTopic = TopicLookupService.getVehicleStatus(nodeId)
      val outTopic = TopicLookupService.getMapperOutput(nodeId, mapperId)
      MapperFunc[Any, VehicleMessage, String, Any](mapperId, inTopic, outTopic, (_, v) => {
        (mapperId: String, v.mph: MPH)
      })
    }
    val reducer = {
      val reducerId = s"$reducerBaseId-$nodeId-$uuid"
      val inTopic = TopicLookupService.getClusterInput(nodeId)
      val outTopic = TopicLookupService.getReducerOutput(nodeId, reducerId)
      KvReducerFunc[String, Double](reducerId, inTopic, outTopic, (a, b) => {
        a + b
      })
    }
    ActiveTransformations(Set(mapper), Set(reducer))
  })
}

object Core extends Simulator with SpeedSumSimulation {
  protected val actorSystem = ActorSystem("SpindleSimulator")

  private def waitUserInput: Unit = {
    println("World initialized. Press ENTER to start")
    scala.io.StdIn.readLine
  }

  private def waitThenStart: Unit = {
    val WAIT_TIME = (5 seconds)
    println(s"World initialized. Auto starting in $WAIT_TIME")
    Thread.sleep(WAIT_TIME.toMillis)
  }

  private def moveResults: Unit = {
    val completedDir = new File(Configuration.simResultsFinishedDir)
    val resultsDir = new File(Configuration.simResultsDir)
    Files.move(resultsDir.toPath, completedDir.toPath, java.nio.file.StandardCopyOption.REPLACE_EXISTING)
  }

  def main(args: Array[String]) {
    println(s"Starting sim ${Configuration.simUid}")
    clearKafka
    initWorld
    waitThenStart
    runSim
    finish
    moveResults
    println("Program Finished. Exiting NSL-Spindle Simulator.")
    System.exit(0)
  }
}