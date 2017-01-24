package edu.rpi.cs.nsl.spindle.vehicle.simulation

import scala.concurrent.duration.DurationInt
import scala.util.Random

import org.scalatest.BeforeAndAfterAll
import org.scalatest.DoNotDiscover
import org.scalatest.WordSpecLike
import org.slf4j.LoggerFactory

import akka.actor.ActorSystem
import akka.actor.Props
import akka.pattern.ask
import akka.actor.actorRef2Scala
import akka.testkit.ImplicitSender
import akka.testkit.TestActorRef
import akka.testkit.TestKit
import edu.rpi.cs.nsl.spindle.vehicle.Types.NodeId
import edu.rpi.cs.nsl.spindle.vehicle.Types.Timestamp
import edu.rpi.cs.nsl.spindle.vehicle.kafka.ClientFactoryDockerFixtures
import edu.rpi.cs.nsl.spindle.vehicle.kafka.DockerHelper
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.CacheFactory
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.postgres.PgClient
import edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations.TransformationStore
import edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations.TransformationStoreFactory
import java.util.concurrent.Executors
import akka.util.Timeout
import org.scalatest.Ignore
import edu.rpi.cs.nsl.spindle.tags.LoadTest
import org.scalatest.Tag
import edu.rpi.cs.nsl.spindle.tags.UnderConstructionTest
import scala.concurrent.Await
import akka.actor.PoisonPill
import akka.actor.ActorRef
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration
import org.scalatest.FlatSpec
import edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations.MapperFunc
import edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations.GenerativeStaticTransformationFactory
import edu.rpi.cs.nsl.spindle.vehicle.kafka.TestObj
import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.TopicLookupService
import edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations.ActiveTransformations
import edu.rpi.cs.nsl.spindle.datatypes.{ Vehicle => VehicleMessage }

trait VehicleExecutorFixtures {
  private type TimeSeq = List[Timestamp]
  private val random = new Random()

  val FIVE_SECONDS_MS: Double = 5 * 1000 toDouble
  lazy val startTime: Timestamp = System.currentTimeMillis() + Configuration.simStartOffsetMs
  val randomTimings: TimeSeq = {
    //val INTERVAL_MS = 1000 // Corresponding to 1s test intervals //TODO: configure window size kafka streams
    val INTERVAL_MS = 30 * 1000 // 30 seconds
    val END_TIME_OFFSET = INTERVAL_MS * 10
    (0 to END_TIME_OFFSET by INTERVAL_MS).map(_.toLong).toList
  }
  val clientFactory = ClientFactoryDockerFixtures.getFactory
  val cacheFactory = new CacheFactory(new PgClient()) {
    override def mkCaches(nodeId: NodeId) = {
      val (_, caches) = super.mkCaches(nodeId)
      (randomTimings, caches)
    }
  }
  private val vehicleProps = ReflectionFixtures.basicPropertiesCollection.toSet.filterNot(_.getTypeString.contains("VehicleId"))
  val nodeId = 0
  val emptyTransformFactory = new EmptyStaticTransformationFactory()
  def mkVehicle(transformationStore: TransformationStore = emptyTransformFactory.getTransformationStore(nodeId)) = new Vehicle(nodeId, clientFactory, transformationStore, cacheFactory, Set(), Set(), false)
  def mkVehicleProps(nodeId: NodeId, fullInit: Boolean = false, transformFactory: TransformationStoreFactory = emptyTransformFactory) = {
    Vehicle.props(nodeId, clientFactory, transformFactory.getTransformationStore(nodeId), cacheFactory, Set(), vehicleProps, fullInit)
  }
}

class VehicleActorSpecDocker extends TestKit(ActorSystem("VehicleActorSpec"))
    with ImplicitSender with WordSpecLike with BeforeAndAfterAll {
  private val logger = LoggerFactory.getLogger(this.getClass)
  override def beforeAll {
    super.beforeAll
    ClientFactoryDockerFixtures.waitReady
  }
  override def afterAll {
    shutdown()
    DockerHelper.stopCluster
  }

  def testNestedExecutors(numVehicles: Int, numThreads: Int, sleepTime: Long)(implicit fixtures: VehicleExecutorFixtures) {
    def mkExecVehicle(nodeId: NodeId) = {
      system.actorOf(Props(new Vehicle(nodeId,
        fixtures.clientFactory,
        fixtures.emptyTransformFactory.getTransformationStore(nodeId),
        fixtures.cacheFactory,
        Set(),
        Set(), false) {
        override def startSimulation(startTime: Timestamp, replyWhenDone: Option[ActorRef]) {
          def getRunnable(threadId: Int) = new Thread() {
            override def run {
              for (i <- 1 to 2) {
                Thread.sleep(sleepTime)
              }
              logger.info(s"$nodeId finished thread $threadId")
            }
          }
          val threads = (0 until numThreads).map(getRunnable)
          threads.foreach(context.dispatcher.execute)
          logger.info(s"Inner executors started for node $nodeId")
        }
      }))
    }
    import scala.concurrent.ExecutionContext.Implicits.global
    implicit val timeout = Timeout(2 minutes)
    (0 to numVehicles).map(mkExecVehicle).par
      .map(actorRef => (actorRef ? Vehicle.CheckReadyMessage) map { case _ => actorRef })
      .map(_ map {
        case actorRef =>
          actorRef ? Vehicle.StartMessage(fixtures.startTime) map {
            case _ => {
              logger.debug(s"$actorRef started")
              actorRef
            }
          }

      })
      .map(Await.result(_, 2 minutes))
      .map(Await.result(_, 2 minutes))
      .toList: List[ActorRef] //TODO: send actor for getting back completion confirmations
  }

  private def fullyStartVehicle(actorRef: ActorRef)(implicit fixtures: VehicleExecutorFixtures): ActorRef = {
    import fixtures._
    implicit val timeout = Timeout(1 minutes)
    val rdyMsg = Await.result(actorRef ? Vehicle.CheckReadyMessage, 1 minutes)
    assert(rdyMsg.equals(Vehicle.ReadyMessage(nodeId)))

    val waitDoneTime: FiniteDuration = (30 + ((fixtures.randomTimings.last + Configuration.simStartOffsetMs) / 1000).toInt) seconds

    val strtMsg = Await.result(actorRef ? Vehicle.StartMessage(startTime, Some(self.actorRef)), 1 minutes)
    assert(strtMsg.isInstanceOf[Vehicle.StartingMessage])

    within(waitDoneTime) {
      logger.info(s"Started vehicle actor from ${self.actorRef} and waiting for $waitDoneTime")
      expectMsgType[Vehicle.SimulationDone]
      logger.info(s"Vehicle finished")
    }
    actorRef
  }

  "A Vehicle actor" should {
    "correctly generate timings" in new VehicleExecutorFixtures {
      val vExec = TestActorRef(mkVehicle()).underlyingActor
      val timings = vExec.mkTimings(startTime)
      assert(timings(0) == timings.min, "First value is not minimum")
      assert(timings.last == timings.max, "Last value is not maximum")
      assert(timings.min == startTime)
      assert(timings.max == (randomTimings.max + startTime),
        s"${timings.max} != ${randomTimings.max + startTime}")
    }
    "spawn multiple copies" taggedAs (LoadTest) in new VehicleExecutorFixtures {
      val NUM_COPIES = 5000
      (0 to NUM_COPIES)
        .map { nodeId =>
          system.actorOf(mkVehicleProps(nodeId))
        }
        .foreach { actor =>
          logger.info(s"Sending test message to $actor")
          within(5 minutes) {
            actor ! Ping()
            expectMsg(Ping())
          }
        }
    }

    "spawn executors inside many vehicles" taggedAs (LoadTest) in new VehicleExecutorFixtures {
      within(10 minutes) {
        testNestedExecutors(numVehicles = 3000, numThreads = 5, sleepTime = 1000)(this)
      }
    }

    "spawn executors inside vehicles" in new VehicleExecutorFixtures {
      within(5 minutes) {
        testNestedExecutors(numVehicles = 10, numThreads = 10, sleepTime = 1000)(this)
      }
    }

    "completely initialize and start" in new VehicleExecutorFixtures {
      val actorRef = system.actorOf(mkVehicleProps(nodeId, fullInit = true))
      fullyStartVehicle(actorRef)(this)
    }

    "completely initialize and run mapper" taggedAs (UnderConstructionTest) in new VehicleExecutorFixtures {
      val mapperId: String = java.util.UUID.randomUUID.toString
      val testMapperFactory = new GenerativeStaticTransformationFactory(nodeId => {
        val inTopic = TopicLookupService.getVehicleStatus(nodeId)
        val outTopic = TopicLookupService.getMapperOutput(nodeId, mapperId)
        val mappers = Set(MapperFunc[Any, VehicleMessage, TestObj, TestObj](mapperId, inTopic, outTopic, (k, v) => {
          logger.info(s"Got vehicle $nodeId message $k -> $v")
          (new TestObj("mappedKey"), new TestObj("mappedValue"))
        }))
          .asInstanceOf[Set[MapperFunc[Any, Any, Any, Any]]]
        ActiveTransformations(mappers, Set())
      })
      val vehicleProps = mkVehicleProps(nodeId: NodeId, fullInit = true, transformFactory = testMapperFactory)
      val actorRef = system.actorOf(vehicleProps)
      fullyStartVehicle(actorRef)(this)
      actorRef ! PoisonPill
      //TODO?
    }

  }

}
