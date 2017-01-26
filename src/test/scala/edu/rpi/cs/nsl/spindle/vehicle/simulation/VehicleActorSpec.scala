package edu.rpi.cs.nsl.spindle.vehicle.simulation

import scala.concurrent.Await
import scala.concurrent.Promise
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration
import scala.util.Random

import org.scalatest.BeforeAndAfterAll
import org.scalatest.DoNotDiscover
import org.scalatest.WordSpecLike
import org.slf4j.LoggerFactory

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.PoisonPill
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.pattern.ask
import akka.testkit.ImplicitSender
import akka.testkit.TestActorRef
import akka.testkit.TestKit
import akka.util.Timeout
import edu.rpi.cs.nsl.spindle.datatypes.{ Vehicle => VehicleMessage }
import edu.rpi.cs.nsl.spindle.tags.LoadTest
import edu.rpi.cs.nsl.spindle.tags.UnderConstructionTest
import edu.rpi.cs.nsl.spindle.vehicle.Types.NodeId
import edu.rpi.cs.nsl.spindle.vehicle.Types.Timestamp
import edu.rpi.cs.nsl.spindle.vehicle.kafka.ClientFactoryDockerFixtures
import edu.rpi.cs.nsl.spindle.vehicle.kafka.DockerHelper
import edu.rpi.cs.nsl.spindle.vehicle.kafka.TestObj
import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.TopicLookupService
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.postgres.PgClient
import edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations.ActiveTransformations
import edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations.GenerativeStaticTransformationFactory
import edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations.MapperFunc
import edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations.TransformationStore
import edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations.TransformationStoreFactory
import edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations.KvReducerFunc
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.PgCacheLoader
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.TSEntryCache
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.ClusterMembership
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.CacheTypes
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.EventStore

trait VehicleExecutorFixtures {
  implicit val ec: ExecutionContext
  private type TimeSeq = List[Timestamp]
  private val random = new Random()

  val FIVE_SECONDS_MS: Double = 5 * 1000 toDouble
  lazy val startTime: Timestamp = System.currentTimeMillis() + Configuration.simStartOffsetMs
  val randomTimings: TimeSeq = {
    val INTERVAL_MS = 1000 // Corresponding to 1s test intervals //TODO: configure window size kafka streams
    val END_TIME_OFFSET = INTERVAL_MS * 10
    (0 to END_TIME_OFFSET by INTERVAL_MS).map(_.toLong).toList
  }
  class MockTimingCacheLoader extends PgCacheLoader {
    override def mkCaches(nodeId: NodeId) = {
      val (_, caches) = super.mkCaches(nodeId)
      (randomTimings, caches)
    }
  }
  def getTestEventStore = new MockTimingCacheLoader()
  val clientFactory = ClientFactoryDockerFixtures.getFactory
  private val vehicleProps = ReflectionFixtures.basicPropertiesCollection.toSet.filterNot(_.getTypeString.contains("VehicleId"))
  val nodeId = 0
  val emptyTransformFactory = new EmptyStaticTransformationFactory()
  def mkVehicle(transformationStore: TransformationStore = emptyTransformFactory.getTransformationStore(nodeId),
                eventStore: EventStore = getTestEventStore) =
    new Vehicle(nodeId, clientFactory, transformationStore, eventStore, Set(), Set(), false)
  def mkVehicleProps(nodeId: NodeId, fullInit: Boolean = false,
                     transformFactory: TransformationStoreFactory = emptyTransformFactory,
                     eventStore: EventStore = getTestEventStore) = {
    Vehicle.props(nodeId, clientFactory, transformFactory.getTransformationStore(nodeId), eventStore, Set(), vehicleProps, fullInit)
  }

  val ZERO_TIME = 0 milliseconds
  private lazy val nodeList = getTestEventStore.getNodes
  private lazy val clusterHeadId = nodeList.head

  class MockClusterCacheLoader extends MockTimingCacheLoader() {
    private lazy val clusterCache = new TSEntryCache[NodeId](Seq(new ClusterMembership(ZERO_TIME, clusterHeadId)))
    override def mkCaches(nodeId: NodeId) = {
      val (timestamps, caches) = super.mkCaches(nodeId)
      // Replace default cluster cache
      (timestamps, (caches - CacheTypes.ClusterCache) +
        (CacheTypes.ClusterCache -> clusterCache))
    }
  }

  def mkCluster(numChildren: Int): (Props, Iterable[Props]) = {
    def mkClusterVehicleProps(nodeId: NodeId) = mkVehicleProps(nodeId, fullInit = true, eventStore = new MockClusterCacheLoader())
    (mkClusterVehicleProps(clusterHeadId), nodeList.tail.take(numChildren).map(mkClusterVehicleProps))
  }
}

class VehicleActorSpecDocker extends TestKit(ActorSystem("VehicleActorSpec"))
    with ImplicitSender with WordSpecLike with BeforeAndAfterAll {
  private val logger = LoggerFactory.getLogger(this.getClass)
  override def beforeAll {
    super.beforeAll
    logger.debug("Vehicle actor spec waiting for docker to come online")
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
        fixtures.getTestEventStore,
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

  //TODO: create consumer to ensure mapper is producing correct output (shld be fine given other tests)
  private def mkTestMappers(nodeId: NodeId, mapperGotMessage: Promise[(_, _)], mapperId: String) = {
    val inTopic = TopicLookupService.getVehicleStatus(nodeId)
    val outTopic = TopicLookupService.getMapperOutput(nodeId, mapperId)
    val mappers = Set(MapperFunc[Any, VehicleMessage, TestObj, TestObj](mapperId, inTopic, outTopic = outTopic, (k, v) => {
      logger.info(s"Got vehicle $nodeId message $k -> $v")
      if (mapperGotMessage.isCompleted == false) {
        mapperGotMessage.success((k, v))
      }
      (new TestObj("mappedKey"), new TestObj("mappedValue"))
    }))
      .asInstanceOf[Set[MapperFunc[Any, Any, Any, Any]]]
    (outTopic, mappers)
  }

  private def mkTestReducers(nodeId: NodeId, reducerGotMessage: Promise[(_, _)], inTopic: String, reducerId: String) = {
    logger.debug(s"Reducer $reducerId listening on topic $inTopic")
    val outTopic = TopicLookupService.getReducerOutput(nodeId, reducerId)
    val reducer = KvReducerFunc[TestObj, TestObj](reducerId, inTopic = inTopic, outTopic: String, (v1, v2) => {
      logger.info(s"Reducer got message ${v1} and ${v2}")
      if (reducerGotMessage.isCompleted == false) {
        reducerGotMessage.success((v1, v2))
      }
      new TestObj(s"${v1.testVal}|${v2.testVal}")
    })
    Set(reducer).asInstanceOf[Set[KvReducerFunc[Any, Any]]]
  }

  private def mkTestMapperFactory(mapperGotMessage: Promise[(_, _)]) = {
    val mapperId: String = java.util.UUID.randomUUID.toString
    new GenerativeStaticTransformationFactory(nodeId => {
      val (_, mappers) = mkTestMappers(nodeId, mapperGotMessage, mapperId)
      ActiveTransformations(mappers, Set())
    })
  }

  private def mkTestMapReduceFactory(mapperGotMessage: Promise[(_, _)], reducerGotMessage: Promise[(_, _)]) = {
    val mapperId: String = java.util.UUID.randomUUID.toString
    val reducerId: String = java.util.UUID.randomUUID.toString
    val factory = new GenerativeStaticTransformationFactory(nodeId => {
      val (mapperOut, mappers) = mkTestMappers(nodeId, mapperGotMessage, mapperId)
      val reducers = mkTestReducers(nodeId, reducerGotMessage, mapperOut, reducerId)
      ActiveTransformations(mappers, reducers)
    })
    //TODO: add consumer to ensure reducer is producing data correctly
    factory
  }

  "A Vehicle actor" should {
    "correctly generate timings" in new VehicleExecutorFixtures {
      implicit val ec = system.dispatcher
      val vExec = TestActorRef(mkVehicle()).underlyingActor
      val timings = vExec.mkTimings(startTime)
      val wallTimings = timings.map(_.wallTime)
      val simTimings = timings.map(_.simTime)
      assert(timings.length == randomTimings.length, "Generated timings don't match random timings length")
      assert(wallTimings(0) == wallTimings.min, "First value is not wall minimum")
      assert(simTimings(0) == simTimings.min, "First value is not sim minimum")
      assert(wallTimings.last == wallTimings.max, "Last value is not wall maximum")
      assert(simTimings.last == simTimings.max, "Last value is not sim maximum")
      assert(wallTimings.min == startTime)
      assert(wallTimings.max == (randomTimings.max + startTime),
        s"${wallTimings.max} != ${randomTimings.max + startTime}\n\tRandom: $randomTimings\n\tWall: $wallTimings")
    }
    "spawn multiple copies" taggedAs (LoadTest) in new VehicleExecutorFixtures {
      implicit val ec = system.dispatcher
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
      implicit val ec = system.dispatcher
      within(10 minutes) {
        testNestedExecutors(numVehicles = 3000, numThreads = 5, sleepTime = 1000)(this)
      }
    }

    "spawn executors inside vehicles" in new VehicleExecutorFixtures {
      implicit val ec = system.dispatcher
      within(5 minutes) {
        testNestedExecutors(numVehicles = 10, numThreads = 10, sleepTime = 1000)(this)
      }
    }

    "completely initialize and start" in new VehicleExecutorFixtures {
      implicit val ec = system.dispatcher
      val actorRef = system.actorOf(mkVehicleProps(nodeId, fullInit = true))
      fullyStartVehicle(actorRef)(this)
    }

    "completely initialize and run mapper" in new VehicleExecutorFixtures {
      implicit val ec = system.dispatcher
      val mapperGotMessage = Promise[(_, _)]()
      val testMapperFactory = mkTestMapperFactory(mapperGotMessage)
      val vehicleProps = mkVehicleProps(nodeId: NodeId, fullInit = true, transformFactory = testMapperFactory)
      val actorRef = system.actorOf(vehicleProps)
      fullyStartVehicle(actorRef)(this)
      assert(mapperGotMessage.isCompleted, "Mapper did not get message at expected time")
      actorRef ! PoisonPill
    }

    "completely initialize and run mapper and reducer" in new VehicleExecutorFixtures {
      implicit val ec = system.dispatcher
      val mapperGotMessage = Promise[(_, _)]()
      val reducerGotMessage = Promise[(_, _)]()
      val testMRFactory = mkTestMapReduceFactory(mapperGotMessage, reducerGotMessage)
      val actorRef = system.actorOf(mkVehicleProps(nodeId: NodeId, fullInit = true, transformFactory = testMRFactory))
      fullyStartVehicle(actorRef)(this)
      assert(mapperGotMessage.isCompleted, s"$mapperGotMessage not complete")
      assert(reducerGotMessage.isCompleted, s"$reducerGotMessage not complete")
    }
    "communicate across vehicles in cluster" taggedAs (UnderConstructionTest) in new VehicleExecutorFixtures {
      implicit val ec = system.dispatcher
      val NUM_CLUSTER_CHILDREN = 1
      val (clusterHead, Seq(clusterChild)) = mkCluster(NUM_CLUSTER_CHILDREN)
      println(s"Cluster head $clusterHead")
      println(s"Cluster child $clusterChild")
      fail("Not implemented") //TODO: create mock clustering, wait for messages to pass among vehicles
      //TODO: implement program entrypoint
    }
  }
}
