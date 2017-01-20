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
import edu.rpi.cs.nsl.spindle.tags.CoreTest
import scala.concurrent.Await

trait VehicleExecutorFixtures {
  private type TimeSeq = List[Timestamp]
  private val random = new Random()

  val FIVE_SECONDS_MS: Double = 5 * 1000 toDouble
  lazy val startTime: Timestamp = System.currentTimeMillis() + Configuration.simStartOffsetMs
  val randomTimings: TimeSeq = (0 to 500).map(_.toLong).toList
  val clientFactory = ClientFactoryDockerFixtures.getFactory
  val cacheFactory = new CacheFactory(new PgClient()) {
    override def mkCaches(nodeId: NodeId) = {
      val (_, caches) = super.mkCaches(nodeId)
      (randomTimings, caches)
    }
  }
  val nodeId = 0
  val emptyTransformFactory = new EmptyStaticTransformationFactory()
  def mkVehicle(transformationStore: TransformationStore = emptyTransformFactory.getTransformationStore(nodeId)) = new Vehicle(nodeId, clientFactory, transformationStore, cacheFactory, Set(), Set(), false)
  def mkVehicleProps(nodeId: NodeId, fullInit: Boolean = false, transformFactory: TransformationStoreFactory = emptyTransformFactory) = {
    Vehicle.props(nodeId, clientFactory, transformFactory.getTransformationStore(nodeId), cacheFactory, Set(), Set(), fullInit)
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
        override def startSimulation(startTime: Timestamp) {
          def getRunnable() = new Thread() {
            override def run {
              for (i <- 1 to 2) {
                Thread.sleep(sleepTime)
              }
            }
          }
          val threads = (0 until numThreads).map(_ => getRunnable())
          threads.foreach(context.dispatcher.execute)
          logger.debug(s"Inner executors started for node $nodeId")
        }
      }))
    }
    import scala.concurrent.ExecutionContext.Implicits.global
    implicit val timeout = Timeout(2 minutes)
    (0 to numVehicles).map(mkExecVehicle).par.foreach { actorRef =>
      (actorRef ? Vehicle.CheckReadyMessage) onSuccess {
        case _ =>
          actorRef ! Vehicle.StartMessage(fixtures.startTime)
      }
    }
    receiveN(numVehicles)
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

    "spawn executors inside vehicles" taggedAs (CoreTest) in new VehicleExecutorFixtures {
      within(5 minutes) {
        testNestedExecutors(numVehicles = 10, numThreads = 10, sleepTime = 1000)(this)
      }
    }

    "completely initialize and start" taggedAs (CoreTest) in new VehicleExecutorFixtures {
      val actorRef = system.actorOf(mkVehicleProps(0, fullInit = true))
      implicit val timeout = Timeout(1 minutes)
      val rdyMsg = Await.result(actorRef ? Vehicle.CheckReadyMessage, 1 minutes)
      assert(rdyMsg.equals(Vehicle.ReadyMessage(0)))

      val strtMsg = Await.result(actorRef ? Vehicle.StartMessage(startTime), 1 minutes)
      assert(strtMsg.isInstanceOf[Vehicle.StartingMessage])
      //TODO
      fail("Not completed")
    }
  }

  //TODO: create, run vehicle, ensure it generates expected outputs
  //TODO: test having lots of vehicles running at once
}
