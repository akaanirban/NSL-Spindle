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
import edu.rpi.cs.nsl.spindle.datatypes.VehicleTypes.{Lat, Lon, MPH}

import scala.concurrent.duration._
import org.slf4j.LoggerFactory

import scala.concurrent.{Await, Future, blocking}
import edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations._
import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.TopicLookupService
import edu.rpi.cs.nsl.spindle.datatypes.{VehicleColors, VehicleTypes, Vehicle => VehicleMessage}
import edu.rpi.cs.nsl.spindle.vehicle.Types.NodeId
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
  val WORLD_TIMEOUT = 10 minutes
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
    try {
      Await.result(actorSystem.whenTerminated, 5 minutes)
    } catch {
      case _: java.util.concurrent.TimeoutException => logger.warn("Akka shutdown timed out")
    }
    logger.info("Finished")
  }
}

trait SpeedSumSimulation {
  assert(Configuration.Vehicles.mapReduceConfigName == "speedSum2")
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
      MapperFunc[NodeId, VehicleMessage, String, MPH](mapperId, inTopic, outTopic, (_, v) => {
        ("allNodes", v.mph: MPH)
      })
    }
    val reducer = {
      val reducerId = s"$reducerBaseId-$nodeId-$uuid"
      val inTopic = TopicLookupService.getClusterInput(nodeId)
      val outTopic = TopicLookupService.getReducerOutput(nodeId, reducerId)
      KvReducerFunc[String, MPH](reducerId, inTopic, outTopic, (a, b) => {
        a + b
      })
    }
    ActiveTransformations(Set(mapper), Set(reducer))
  })
}

trait DualQuery {
  assert(Configuration.Vehicles.mapReduceConfigName == "dualQuery")
  import VehicleTypes.MPH
  private def uuid = java.util.UUID.randomUUID.toString
  protected[this] val transformationStoreFactory: TransformationStoreFactory = new GenerativeStaticTransformationFactory(nodeId => {
    val speedMapper = {
      val mapperId = s"getSpeed-mapper-$nodeId-$uuid"
      val inTopic = TopicLookupService.getVehicleStatus(nodeId)
      val outTopic = TopicLookupService.getMapperOutput(nodeId, mapperId)
      MapperFunc[NodeId, VehicleMessage, String, MPH](mapperId, inTopic, outTopic, (_, v) => {
        (mapperId: String, v.mph: MPH)
      })
    }
    val colorMapper = {
      val mapperId = s"getColor-mapper-$nodeId-$uuid"
      val inTopic = TopicLookupService.getVehicleStatus(nodeId)
      val outTopic = TopicLookupService.getMapperOutput(nodeId, mapperId)
      MapperFunc[NodeId, VehicleMessage, String, Set[VehicleColors.Value]](mapperId, inTopic, outTopic, (_, v) => {
        (mapperId: String, Set(v.color: VehicleColors.Value))
      })
    }
    val speedReducer = {
      val reducerId = s"getSpeed-reducer-$nodeId-$uuid"
      val inTopic = TopicLookupService.getClusterInput(nodeId)
      val outTopic = TopicLookupService.getReducerOutput(nodeId, reducerId)
      KvReducerFunc[String, MPH](reducerId, inTopic, outTopic, (a, b) => {
        a + b
      })
    }
    val colorReducer = {
      val reducerId = s"getColor-reducer-$nodeId-$uuid"
      val inTopic = TopicLookupService.getClusterInput(nodeId)
      val outTopic = TopicLookupService.getReducerOutput(nodeId, reducerId)
      KvReducerFunc[String, Set[VehicleColors.Value]](reducerId, inTopic, outTopic, (a, b) => {
        a ++ b
      })
    }
    ActiveTransformations(Set(speedMapper, colorMapper), Set(speedReducer, colorReducer))
  })
}

trait DynamicQuery {
  private def uuid = java.util.UUID.randomUUID.toString


  case class Region(latRange: (Lat, Lat), lonRange: (Lon, Lon), id: String = uuid) {
    def containsVehicle(vehicleMessage: VehicleMessage): Boolean = {
      isInsideBound((vehicleMessage.lat, vehicleMessage.lon), latRange, lonRange)
    }
  }

  private def mkRegionSpeedAvgMapper(nodeId: NodeId, regions: Iterable[Region]) = {
    val mapperId = s"getSpeed-mapper-$nodeId-$uuid"
    val inTopic = TopicLookupService.getVehicleStatus(nodeId)
    val outTopic = TopicLookupService.getMapperOutput(nodeId, mapperId)
    MapperFunc[NodeId, VehicleMessage, String, (MPH, Long)](mapperId, inTopic, outTopic, (_, v) => {
      val region = regions.filter(_.containsVehicle(v)).head
      (region.id: String, (v.mph, 1): (MPH, Long))
    })
  }

  private def mkSendAll(): TransformationStoreFactory = {
    new GenerativeStaticTransformationFactory(nodeId => {
      val mapper ={
        val mapperId = s"sendAll-mapper-$nodeId-$uuid"
        val inTopic = TopicLookupService.getVehicleStatus(nodeId)
        val outTopic = TopicLookupService.getMapperOutput(nodeId, mapperId)
        MapperFunc[NodeId, VehicleMessage, NodeId, VehicleMessage](mapperId, inTopic, outTopic, (k,v) =>{
          (k,v)
        })
      }
      val reducer = {
        val reducerId = s"sendAll-reducer-$nodeId-$uuid"
        val inTopic = TopicLookupService.getClusterInput(nodeId)
        val outTopic = TopicLookupService.getReducerOutput(nodeId, reducerId)
        KvReducerFunc[NodeId, VehicleMessage](reducerId, inTopic, outTopic, (a,b) => {
          a
        })
      }
      ActiveTransformations(Set(mapper), Set(reducer))
    })
  }

  private def mkSpeedAvgReducer(nodeId: NodeId) = {
    val reducerId = s"getSpeed-avg-reducer-$nodeId-$uuid"
    val inTopic = TopicLookupService.getClusterInput(nodeId)
    val outTopic = TopicLookupService.getReducerOutput(nodeId, reducerId)
    KvReducerFunc[String, (MPH, Long)](reducerId, inTopic, outTopic, (a, b) => {
      (a._1 + b._1, a._2 + b._2)
    })
  }

  private def isInsideBound(latLon: (Lat, Lon), latRange: (Lat, Lat), lonRange: (Lon, Lon)): Boolean = {
    val (lat, lon) = latLon
    val (latMin, latMax) = latRange
    val (lonMin, lonMax) = lonRange
    (lat <= latMax && lon <= lonMax) && (lat >= latMin && lon >= lonMin)
  }

  private def mkRegionSpeedFilterMapper(nodeId: NodeId, latRange: (Lat, Lat), lonRange: (Lon, Lon)) = {
    val mapperId = s"getSpeed-filtered-mapper-$nodeId-$uuid"
    val inTopic = TopicLookupService.getVehicleStatus(nodeId)
    val outTopic = TopicLookupService.getMapperOutput(nodeId, mapperId)
    MapperFunc[NodeId, VehicleMessage, String, (MPH, Long)](mapperId, inTopic, outTopic,
      mapFunc = (_, v) => {
        (mapperId, (v.mph, 1): (MPH, Long))
      },
      filterFunc = (_,v) => {
        isInsideBound((v.lat, v.lon), latRange, lonRange)
    })
  }

  // x is lat, y is lon
  // avg(x) = 50502, avg(y) = 101220
  private val DENSE_LAT_RANGE = (5000: Lat, 50500: Lat)
  private val DENSE_LON_RANGE = (100000: Lon, 110000: Lon)

  private val DENSE_REGIONS = {
    val region1 = Region(DENSE_LAT_RANGE, DENSE_LON_RANGE)
    val region2 = Region((DENSE_LAT_RANGE._2, 90500: Lat), DENSE_LON_RANGE)
    Seq(region1, region2)
  }

  private val SPARSE_LAT_RANGE = (40000: Lat, 51297: Lat)
  private val SPARSE_LON_RANGE = (106800: Lon, 114000: Lon)

  private val SPARSE_REGIONS = {
    val region1 = Region(SPARSE_LAT_RANGE, SPARSE_LON_RANGE)
    val (latMin, latMax) = SPARSE_LAT_RANGE
    val region2 = Region((SPARSE_LAT_RANGE._2, (latMin + latMax / 2): Lat), SPARSE_LON_RANGE)
    Seq(region1, region2)
  }

  private def mkGeoFilteredSpeedAvg(latRange: (Lat, Lat), lonRange: (Lon, Lon)): TransformationStoreFactory = {
    new GenerativeStaticTransformationFactory(nodeId => {
      ActiveTransformations(Set(mkRegionSpeedFilterMapper(nodeId, latRange, lonRange)), Set(mkSpeedAvgReducer(nodeId)))
    })
  }

  private def mkGeoMappedSpeedAvg(regions: Iterable[Region]): TransformationStoreFactory = {
    new GenerativeStaticTransformationFactory(nodeId => {
      ActiveTransformations(Set(mkRegionSpeedAvgMapper(nodeId, regions)), Set(mkSpeedAvgReducer(nodeId)))
    })
  }

  //TODO: generate geo filter based on node region filter
  //TODO: clean this up
  lazy protected val transformationStoreFactory: TransformationStoreFactory = Configuration.Vehicles.mapReduceConfigName match {
    case "dualQuery" => (new DualQuery {
      def getXformFactory = this.transformationStoreFactory
    }).getXformFactory
    case "speedSum2" => (new SpeedSumSimulation {
      def getXformFactory = this.transformationStoreFactory
    }).getXformFactory
    case "sendAll" => {
     mkSendAll()
    }
    case "geoFiltered" => {
      Configuration.Vehicles.nodePositionsTable match {
        case "dense_positions" => mkGeoFilteredSpeedAvg(DENSE_LAT_RANGE, DENSE_LON_RANGE)
        case "sparse_positions" => mkGeoFilteredSpeedAvg(SPARSE_LAT_RANGE, SPARSE_LON_RANGE)
      }
    }
    case "geoMapped" => {
      Configuration.Vehicles.nodePositionsTable match {
        case "dense_positions" => mkGeoMappedSpeedAvg(DENSE_REGIONS)
        case "sparse_positions" => mkGeoMappedSpeedAvg(SPARSE_REGIONS)
      }
    }
    case c: Any => throw new RuntimeException(s"Unrecognized configuration: $c")
  }
}

object Core extends Simulator with DynamicQuery {
  private val logger = LoggerFactory.getLogger(this.getClass())
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
  //

  def main(args: Array[String]) {
    import scala.concurrent.ExecutionContext.Implicits.global
    println(s"Starting sim ${Configuration.simUid}")
    clearKafka
    initWorld
    waitThenStart
    runSim
    finish
    System.err.println("Program Finishing. Closing down NSL-Spindle Simulator")
    moveResults

    try {
      Await.ready(Future {
        blocking {
          clearKafka
        }
      }, 2 minutes)
    } catch {
      case _: java.util.concurrent.TimeoutException => logger.warn("Did not clear kafka")
    }
    println("Program Finished. Exiting NSL-Spindle Simulator.")
    System.exit(0)

  }
}