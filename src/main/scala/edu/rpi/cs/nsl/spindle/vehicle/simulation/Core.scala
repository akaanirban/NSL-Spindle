package edu.rpi.cs.nsl.spindle.vehicle.simulation

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
import edu.rpi.cs.nsl.spindle.datatypes.{ Vehicle => VehicleMessage }
import edu.rpi.cs.nsl.spindle.datatypes.VehicleTypes
import edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations.KvReducerFunc

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

trait SpeedSumSimulation {
  import VehicleTypes.MPH
  val baseId = "getSpeed"
  val mapperBaseId = s"$baseId-mapper"
  val reducerBaseId = s"$baseId-reducer"
  protected val transformationStoreFactory: TransformationStoreFactory = new GenerativeStaticTransformationFactory(nodeId => {
    val mapper = {
      val mapperId = s"$mapperBaseId-$nodeId"
      val inTopic = TopicLookupService.getVehicleStatus(nodeId)
      val outTopic = TopicLookupService.getMapperOutput(nodeId, mapperId)
      MapperFunc[Any, VehicleMessage, String, Any](mapperId, inTopic, outTopic, (_, v) => {
        (mapperId: String, v.mph: MPH)
      })
    }
    val reducer = {
      val inTopic = TopicLookupService.getClusterInput(nodeId)
      val outTopic = TopicLookupService.getReducerOutput(nodeId, reducerBaseId)
      KvReducerFunc[String, Double](reducerBaseId, inTopic, outTopic, (a, b) => {
        12
      })
    }
    ActiveTransformations(Set(mapper), Set()) //TODO
  })
}

object Core extends Simulator with SpeedSumSimulation {
  protected val actorSystem = ActorSystem("SpindleSimulator")

  def main(args: Array[String]) {
    initWorld
    println("World initialized. Press ENTER to start")
    scala.io.StdIn.readLine
    runSim
    finish
  }
}