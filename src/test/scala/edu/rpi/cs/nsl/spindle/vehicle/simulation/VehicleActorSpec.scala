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
import scala.concurrent._
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.PgCacheLoader
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.TSEntryCache
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.ClusterMembership
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.CacheTypes
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.EventStore
import scala.concurrent.duration.Duration

trait VehicleExecutorFixtures {
  implicit val ec: ExecutionContext
  private val logger = LoggerFactory.getLogger(this.getClass)
  private type TimeSeq = List[Timestamp]
  private val random = new Random()

  val FIVE_SECONDS_MS: Double = 5 * 1000 toDouble
  lazy val startTime: Timestamp = System.currentTimeMillis() + Configuration.simStartOffsetMs
  lazy val pgLoader = new PgCacheLoader()
  lazy val simTimesMin: Timestamp = pgLoader.mkCaches(NODE_ID_ZERO)._1.min
  val fakeTimingsNodeZero: TimeSeq = {
    val INTERVAL_MS = 1000 // Corresponding to 1s test intervals //TODO: configure window size kafka streams
    val END_TIME_OFFSET = INTERVAL_MS * 10
    val timings = (simTimesMin to simTimesMin + END_TIME_OFFSET by INTERVAL_MS).map(_.toLong).toList
    timings
  }
  class MockTimingCacheLoader extends PgCacheLoader {
    override def mkCaches(nodeId: NodeId) = {
      val (_, caches) = super.mkCaches(nodeId)
      logger.info(s"Creating caches for node $nodeId: $caches")
      (fakeTimingsNodeZero, caches)
    }
  }
  def getTestEventStore = new MockTimingCacheLoader()
  val clientFactory = ClientFactoryDockerFixtures.getFactory
  private val vehicleProps = ReflectionFixtures.basicPropertiesCollection.toSet.filterNot(_.getTypeString.contains("VehicleId"))
  val NODE_ID_ZERO: NodeId = 0
  val emptyTransformFactory = new EmptyStaticTransformationFactory()

  def mkVehicle(transformationStore: TransformationStore = emptyTransformFactory.getTransformationStore(NODE_ID_ZERO),
                eventStore: EventStore = getTestEventStore) =
    new Vehicle(NODE_ID_ZERO, clientFactory, transformationStore, eventStore, Set(), Set(), false)

  def mkVehicleProps(nodeId: NodeId, fullInit: Boolean = false,
                     transformFactory: TransformationStoreFactory = emptyTransformFactory,
                     eventStore: EventStore = getTestEventStore) = {
    Vehicle.props(nodeId,
      clientFactory,
      transformFactory.getTransformationStore(nodeId),
      eventStore,
      Set(),
      vehicleProps,
      fullInit)
  }

  val ZERO_TIME = 0 milliseconds
  lazy val nodeList = getTestEventStore.getNodes

  class MockClusterCacheLoader extends MockTimingCacheLoader() {
    private lazy val clusterCache = new TSEntryCache[NodeId](Seq(new ClusterMembership(ZERO_TIME, NODE_ID_ZERO)))
    override def mkCaches(nodeId: NodeId) = {
      val (timestamps, caches) = super.mkCaches(nodeId)
      // Replace default cluster cache
      val fakedCaches = (caches - CacheTypes.ClusterCache) +
        (CacheTypes.ClusterCache -> clusterCache)
      logger.debug(s"Generated fake caches for $nodeId: $fakedCaches and cluster cache ${fakedCaches(CacheTypes.ClusterCache).cache}")
      (timestamps, fakedCaches)
    }
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

  private def waitDoneTime()(implicit fixtures: VehicleExecutorFixtures) = (10 + ((fixtures.fakeTimingsNodeZero.last - fixtures.fakeTimingsNodeZero.head + Configuration.simStartOffsetMs) / 1000).toInt) seconds

  private def fullyStartVehicleAndWaitDone(actorRef: ActorRef, nodeIdOpt: Option[NodeId] = None)(implicit fixtures: VehicleExecutorFixtures): ActorRef = {
    import fixtures._
    val nodeId = nodeIdOpt match {
      case Some(nodeId) => nodeId
      case None         => fixtures.NODE_ID_ZERO
    }
    implicit val timeout = Timeout(1 minutes)
    val rdyMsg = Await.result(actorRef ? Vehicle.CheckReadyMessage, 1 minutes)
    assert(rdyMsg.equals(Vehicle.ReadyMessage(nodeId)), s"Ready message for node $nodeId was invalid ${rdyMsg}")

    val strtMsg = Await.result(actorRef ? Vehicle.StartMessage(startTime, Some(self.actorRef)), 1 minutes)
    assert(strtMsg.isInstanceOf[Vehicle.StartingMessage], s"Starting message for node $nodeId was invalid ${strtMsg}")

    within(waitDoneTime) {
      logger.info(s"Started vehicle actor from ${self.actorRef} and waiting for $waitDoneTime")
      expectMsgType[Vehicle.SimulationDone]
      logger.info(s"Vehicle finished")
    }
    actorRef
  }

  private def mkMapVal(nodeId: NodeId) = s"mappedValue-$nodeId"

  //TODO: create consumer to ensure mapper is producing correct output (shld be fine given other tests)
  private def mkTestMappers(nodeId: NodeId, mapperGotMessage: Promise[(_, _)], mapperId: String) = {
    val inTopic = TopicLookupService.getVehicleStatus(nodeId)
    val outTopic = TopicLookupService.getMapperOutput(nodeId, mapperId)
    val mappers = Set(MapperFunc[Any, VehicleMessage, TestObj, TestObj](mapperId, inTopic, outTopic = outTopic, (k, v) => {
      logger.info(s"Got vehicle $nodeId message $k -> $v")
      if (mapperGotMessage.isCompleted == false) {
        mapperGotMessage.success((k, v))
      }
      (new TestObj(s"mappedKey-$nodeId"), new TestObj(mkMapVal(nodeId)))
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

  private def mkUUID = java.util.UUID.randomUUID.toString

  private def mkTestMapperFactory(mapperGotMessage: Promise[(_, _)]) = {
    val mapperId: String = mkUUID
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

  def mkCluster(reducerSawChildPromises: Seq[Promise[Unit]])(implicit fixtures: VehicleExecutorFixtures): ((NodeId, Props), Iterable[(NodeId, Props)]) = {
    import fixtures._
    val clusterHeadId = NODE_ID_ZERO
    val numChildren = reducerSawChildPromises.size
    val childIds = pgLoader.getConcurrentNodes(clusterHeadId).take(numChildren)
    val childTransformStore = mkTestMapperFactory(Promise[(_, _)]())
    val promiseMap = childIds.map(mkMapVal).zip(reducerSawChildPromises).toMap
    val clusterHeadTransformStore = new GenerativeStaticTransformationFactory(nodeId => {
      val (_, mappers) = mkTestMappers(nodeId, Promise[(_, _)](), mkUUID)
      val reducerTopic = TopicLookupService.getClusterInput(nodeId)
      val outTopic = TopicLookupService.getClusterOutput(nodeId)
      val reducer = KvReducerFunc[TestObj, TestObj](mkUUID, inTopic = reducerTopic, outTopic: String, (v1, v2) => {
        val v1match = promiseMap.filterKeys(_ == v1.testVal)
        val v2match = promiseMap.filterKeys(_ == v2.testVal)
        (v1match ++ v2match).filterNot { case (_, promise) => promise.isCompleted }
          .values
          .foreach(_.success(Unit))
        logger.debug(s"Reducer from $reducerTopic -> $outTopic node $nodeId got (${v1.testVal}, ${v2.testVal}")
        new TestObj(s"reducer-output")
      })

      ActiveTransformations(mappers, Set(reducer).asInstanceOf[Set[KvReducerFunc[Any, Any]]])
    })
    def mkClusterVehicleProps(nodeId: NodeId, transformFactory: TransformationStoreFactory = childTransformStore) = {
      val vehicleProps = mkVehicleProps(nodeId, fullInit = true, eventStore = new MockClusterCacheLoader(), transformFactory = transformFactory)
      (nodeId, vehicleProps)
    }
    (mkClusterVehicleProps(clusterHeadId, clusterHeadTransformStore), childIds.map(mkClusterVehicleProps(_)))
  }

  "A Vehicle actor" should {
    "correctly generate timings" in new VehicleExecutorFixtures {
      implicit val ec = system.dispatcher
      val vExec = TestActorRef(mkVehicle()).underlyingActor
      val timings = vExec.mkTimings(startTime)
      val wallTimings = timings.map(_.wallTime)
      val simTimings = timings.map(_.simTime)
      assert(timings.length == fakeTimingsNodeZero.length, "Generated timings don't match random timings length")
      assert(wallTimings(0) == wallTimings.min, "First value is not wall minimum")
      assert(simTimings(0) == simTimings.min, "First value is not sim minimum")
      assert(wallTimings.last == wallTimings.max, "Last value is not wall maximum")
      assert(simTimings.last == simTimings.max, "Last value is not sim maximum")
      assert(wallTimings.min == startTime)
      assert(wallTimings.max == (fakeTimingsNodeZero.max + startTime - fakeTimingsNodeZero.min),
        s"Wall timings max not correct\n\tStart Time: $startTime\n\tRandom: $fakeTimingsNodeZero\n\tWall: $wallTimings")
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
      val actorRef = system.actorOf(mkVehicleProps(NODE_ID_ZERO, fullInit = true))
      fullyStartVehicleAndWaitDone(actorRef)(this)
    }

    "completely initialize and run mapper" in new VehicleExecutorFixtures {
      implicit val ec = system.dispatcher
      val mapperGotMessage = Promise[(_, _)]()
      val testMapperFactory = mkTestMapperFactory(mapperGotMessage)
      val vehicleProps = mkVehicleProps(NODE_ID_ZERO: NodeId, fullInit = true, transformFactory = testMapperFactory)
      val actorRef = system.actorOf(vehicleProps)
      fullyStartVehicleAndWaitDone(actorRef)(this)
      assert(mapperGotMessage.isCompleted, "Mapper did not get message at expected time")
      actorRef ! PoisonPill
    }

    "completely initialize and run mapper and reducer" in new VehicleExecutorFixtures {
      implicit val ec = system.dispatcher
      val mapperGotMessage = Promise[(_, _)]()
      val reducerGotMessage = Promise[(_, _)]()
      val testMRFactory = mkTestMapReduceFactory(mapperGotMessage, reducerGotMessage)
      val actorRef = system.actorOf(mkVehicleProps(NODE_ID_ZERO: NodeId, fullInit = true, transformFactory = testMRFactory))
      fullyStartVehicleAndWaitDone(actorRef)(this)
      assert(mapperGotMessage.isCompleted, s"$mapperGotMessage not complete")
      assert(reducerGotMessage.isCompleted, s"$reducerGotMessage not complete")
    }
    "communicate across vehicles in cluster" in new VehicleExecutorFixtures {
      implicit val timeout = Timeout(1 minutes)
      implicit val ec = system.dispatcher
      implicit val fixtures = this
      val reducerPromises = Seq(Promise[Unit]())
      logger.debug("Creating cluster props")
      val (clusterHead, clusterChildren) = mkCluster(reducerPromises)
      logger.debug("Creating actors")
      val actors = (Seq(clusterHead) ++ clusterChildren)
        .map {
          case (nodeId, actorProps) =>
            (nodeId, system.actorOf(actorProps))
        }
      assert(actors.size > 1)
      assert(reducerPromises.size == clusterChildren.size)
      logger.debug("Waiting for actors to come online")
      val readyFuture = Future.sequence {
        actors
          .map {
            case (nodeId, actorRef) =>
              actorRef ? Vehicle.CheckReadyMessage
          }
      }

      Await.result(readyFuture, 1 minutes)
      logger.info("All nodes ready")
      val startedFuture = Future.sequence { actors.map { case (_, actorRef) => actorRef ? Vehicle.StartMessage(startTime, Some(self.actorRef)) } }
      Await.result(startedFuture, 1 minutes)
      logger.info("All nodes started")
      val promiseResults = Await.result(Future.sequence(reducerPromises.map(_.future)), waitDoneTime)
      logger.info("Promise results: $promiseResults")
      assert(reducerPromises.forall(_.isCompleted), "Cluster head did not get outputs from all nodes")
    }
  }
}
