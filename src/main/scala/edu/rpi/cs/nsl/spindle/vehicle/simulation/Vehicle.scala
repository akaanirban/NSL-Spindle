package edu.rpi.cs.nsl.spindle.vehicle.simulation

import java.util.concurrent.TimeUnit

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration

import akka.actor._
import akka.actor.Actor._
import akka.actor.Props
import akka.event.Logging
import akka.pattern.ask
import edu.rpi.cs.nsl.spindle.datatypes.{ Vehicle => VehicleMessage }
import edu.rpi.cs.nsl.spindle.datatypes.VehicleTypes
import scala.concurrent.duration._
import edu.rpi.cs.nsl.spindle.vehicle.Types._
import edu.rpi.cs.nsl.spindle.vehicle.kafka.ClientFactory
import edu.rpi.cs.nsl.spindle.vehicle.kafka.streams.StreamExecutor
import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.TopicLookupService
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.CacheTypes
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.EventStore
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.Position
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.TSEntryCache
import edu.rpi.cs.nsl.spindle.vehicle.simulation.sensors.MockSensor
import edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations.ActiveTransformations
import edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations.KvReducerFunc
import edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations.MapperFunc
import edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations.TransformationFunc
import edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations.TransformationStore
import akka.util.Timeout
import edu.rpi.cs.nsl.spindle.vehicle.TypedValue

case class Ping()

object Vehicle {
  case class StartMessage(startTime: Timestamp, replyWhenDone: Option[ActorRef] = None)
  case class ReadyMessage(nodeId: NodeId)
  case class StartingMessage(nodeId: NodeId, eventTime: Timestamp = System.currentTimeMillis)
  case class FullShutdown()
  // Sent by world
  case class CheckReadyMessage()
  def props(nodeId: NodeId,
            clientFactory: ClientFactory,
            transformationStore: TransformationStore,
            eventStore: EventStore,
            mockSensors: Set[MockSensor[Any]],
            properties: Set[TypedValue[Any]],
            warmCaches: Boolean = true): Props = {
    Props(new Vehicle(nodeId: NodeId,
      clientFactory: ClientFactory,
      transformationStore: TransformationStore,
      eventStore: EventStore,
      mockSensors: Set[MockSensor[Any]],
      properties: Set[TypedValue[Any]],
      warmCaches))
  }
  // Sent when execution completed
  case class SimulationDone(nodeId: NodeId)
}

object SchedulerUtils { //TODO: remove
  implicit class EnhancedScheduler(scheduler: Scheduler) {
    def scheduleAfterTime(timestamp: Timestamp, f: => Unit)(implicit ec: ExecutionContext) {
      val delayMs = (timestamp - System.currentTimeMillis()).toLong
      assert(delayMs >= 0, s"Time $timestamp has already passed (current time ${System.currentTimeMillis})")
      scheduler.scheduleOnce(Duration.create(delayMs, TimeUnit.MILLISECONDS))(f)
    }
  }
}

/**
 * Simulates an individual vehicle
 *
 * @todo - have real-world "vehicle" talk to simulator wrapper
 *
 * @todo - convert "sensors" to TSEntryCache? (refactor to expect data sometimes to be generated)
 *
 */
class Vehicle(nodeId: NodeId,
              clientFactory: ClientFactory,
              transformationStore: TransformationStore,
              eventStore: EventStore,
              mockSensors: Set[MockSensor[Any]],
              properties: Set[TypedValue[Any]],
              // Disable cache warming for faster tests
              warmCaches: Boolean = true)
    extends Actor with ActorLogging { //TODO: kafka references
  private lazy val logger = Logging(context.system, this)
  private lazy val fullProperties: Iterable[TypedValue[Any]] = {
    properties.toSeq ++
      Seq(TypedValue[VehicleTypes.VehicleId](nodeId)).asInstanceOf[Seq[TypedValue[Any]]]
  }
  private lazy val (timestamps, caches): (Seq[Timestamp], CacheMap) = eventStore.mkCaches(nodeId)

  /**
   * Actor responsible for relaying data to cluster head
   */
  private lazy val clusterHeadConnection: ActorRef = {
    logger.debug(s"Node $nodeId creating cluster head connection actor")
    val actorRef: ActorRef = context.actorOf(VehicleConnection.props(nodeId, clientFactory.getConfig), name = s"clusterHeadConnector$nodeId")
    context.watch(actorRef)
    logger.debug(s"Node $nodeId has created cluster head connection actor")
    //NOTE: insert monitoring here (once message-based tick is set up)
    actorRef
  }

  private lazy val middlewareConnection = {
    logger.debug(s"$nodeId creating middleware connection relay")
    val inTopic = TopicLookupService.getClusterOutput(nodeId)
    val outTopic = TopicLookupService.middlewareInput
    val relayId = s"$nodeId-middleware-connection"
    val relay = clientFactory.mkRelay(Set(inTopic), outTopic, relayId)
    logger.debug(s"$nodeId starting middleware relay")
    relay.run
    logger.debug(s"$nodeId has started middleware relay")
    relay
  }

  /**
   * Create a Vehicle status message
   */
  private[simulation] def generateMessage(currentSimTime: Timestamp): VehicleMessage = {
    import VehicleTypes._
    import CacheTypes._
    val readings: Seq[TypedValue[Any]] = {
      val positionSeq = caches(PositionCache)
        .asInstanceOf[TSEntryCache[Position]]
        .getValueOpt(currentSimTime)
        .map { position =>
          val mph = TypedValue[MPH](position.speed)
          val lat = TypedValue[Lat](position.x)
          val lon = TypedValue[Lon](position.y)
          Seq[TypedValue[_]](mph, lat, lon)
        }
        .getOrElse {
          logger.error(s"No position information available for time $currentSimTime in timestamps $timestamps for cache ${caches(PositionCache).cache}")
          throw new RuntimeException(s"No position information for $currentSimTime")
        }

      mockSensors.toSeq.map(_.getReading(currentSimTime)) ++ positionSeq
    }.asInstanceOf[Seq[TypedValue[Any]]]
    VehicleMessageFactory.mkVehicle(readings, fullProperties)
  }

  private[simulation] case class TimeMapping(wallTime: Timestamp, simTime: Timestamp)

  /**
   * Create mapping from simulator time to future epoch times based on startTime
   */
  private[simulation] def mkTimings(startTime: Timestamp): Seq[TimeMapping] = {
    val zeroTime = eventStore.getMinSimTime
    val offsets = timestamps.map(_ - zeroTime)
    assert(offsets.length == timestamps.length)
    assert(offsets(0) == 0)
    val absoluteTimes = offsets
      .map(_ + startTime)
      .sorted
      .zip(timestamps)
      .sorted
      .map { case (wallTime, simTime) => TimeMapping(wallTime, simTime) }
    assert(absoluteTimes.head.wallTime == startTime, s"Start time $startTime does not map to ${absoluteTimes.head}")
    assert(zeroTime <= timestamps.min, s"${timestamps.min} less than zero time $zeroTime")
    // Ensure ascending order
    absoluteTimes
  }

  private def currentTime: Timestamp = System.currentTimeMillis

  private def sleepUntil(epochTime: Timestamp) {
    val sleepMs = epochTime - currentTime
    if (sleepMs <= 0) {
      logger.warning(s"Next time interval has already elapsed $epochTime")
    } else {
      Thread.sleep(sleepMs)
    }
  }

  private trait TemporalDaemon {
    def executeInterval(currentSimTime: Timestamp): Unit
    def safeShutdown: Unit
    def fullShutdown {}
  }

  private object SensorDaemon extends TemporalDaemon {
    private lazy val outTopic = TopicLookupService.getVehicleStatus(nodeId)
    private val producer = clientFactory.mkProducer[NodeId, VehicleMessage](outTopic)
    def executeInterval(currentSimTime: Timestamp) {
      logger.debug(s"$nodeId executing sensor daemon for $currentSimTime")
      val message = generateMessage(currentSimTime)
      logger.debug(s"$nodeId generated message $message")
      producer.send(nodeId, message)
      logger.debug(s"$nodeId sent status message")
    }
    def safeShutdown: Unit = {
      logger.debug(s"$nodeId shutting down sensor producer")
      producer.flush
      producer.close
      logger.debug(s"$nodeId has shut down sensor producer")
    }
  }

  implicit val connectionActorTimeout = Timeout(2 seconds) //TODO: this should be within 1 second

  private object ClusterMembershipDaemon extends TemporalDaemon {
    private lazy val clusterCacheRef = caches(CacheTypes.ClusterCache).asInstanceOf[TSEntryCache[NodeId]]
    def executeInterval(currentSimTime: Timestamp) {
      clusterCacheRef.getOrPriorOpt(currentSimTime) match {
        case Some(clusterHead) => {
          logger.info(s"Node $nodeId has cluster head $clusterHead at time $currentSimTime")
          Await.result((clusterHeadConnection ? VehicleConnection.SetClusterHead(clusterHead)), 2 seconds)
          logger.info(s"Node $nodeId has updated cluster head $clusterHead at time $currentSimTime")
        }
        case None => {
          logger.warning(s"No cluster head found for node $nodeId at time $currentSimTime: ${clusterCacheRef.cache}")
        }
      }
    }
    def safeShutdown: Unit = {
      logger.debug(s"$nodeId is shutting down cluster-head relay")
      clusterHeadConnection ! VehicleConnection.CloseConnection()
    }
  }

  private object MapReduceDaemon extends TemporalDaemon {
    private lazy val pool = context.dispatcher //TODO: replace with Executors.newCachedThreadPool?
    private var prevMappers: Map[MapperFunc[_, _, _, _], StreamExecutor] = Map()
    private var prevReducers: Map[KvReducerFunc[_, _], StreamExecutor] = Map()
    private def getChanges[T <: TransformationFunc](existing: Set[T], latest: Set[T]) = {
      val toRemove = existing diff latest
      val toAdd = latest diff existing
      (toRemove, toAdd)
    }
    private def updateTransformers[T <: TransformationFunc](toRun: Iterable[T], prevMap: Map[T, StreamExecutor]): Map[T, StreamExecutor] = {
      val (toRemove, toAdd) = getChanges(prevMap.keySet, toRun.toSet)
      toRemove.map(prevMap(_)).foreach(_.stopStream)
      val newTransformers = toAdd.map(func => (func -> func.getTransformExecutor(clientFactory))).toMap
      newTransformers.values.foreach(_.run)
      newTransformers.values.foreach(transformer => logger.debug(s"Launched executor $transformer"))
      (prevMap -- toRemove) ++ newTransformers
    }
    private def updateMappers(mappers: Iterable[MapperFunc[_, _, _, _]]) {
      val nextMap = updateTransformers(mappers, prevMappers)
      assert(nextMap.keySet equals mappers.toSet)
      prevMappers = nextMap
    }
    private def updateReducers(reducers: Iterable[KvReducerFunc[_, _]]) {
      val nextReducers = updateTransformers(reducers, prevReducers)
      assert(nextReducers.keySet equals reducers.toSet)
      prevReducers = nextReducers
    }
    def executeInterval(currentSimTime: Timestamp) {
      logger.debug(s"$nodeId mapreduce daemon updating for $currentSimTime")
      val ActiveTransformations(mappers, reducers) = transformationStore
        .getActiveTransformations(currentSimTime)
      logger.debug(s"$nodeId updating mappers")
      updateMappers(mappers)
      logger.debug(s"$nodeId updating reducers")
      updateReducers(reducers)
      logger.debug(s"$nodeId updating cluster head connection with map/reduce changes")
      Await.result((clusterHeadConnection ? VehicleConnection.SetMappers(mappers.map(_.funcId).toSet)), Duration.Inf) //TODO: clean up 
      logger.info(s"$nodeId updated mappers and reducers: $prevMappers $prevReducers")
    }
    private def shutdownReducers: Unit = {
      logger.debug(s"$nodeId shutting down reducers")
      prevReducers.values.foreach(_.stopStream)
    }
    def safeShutdown: Unit = {
      logger.debug(s"$nodeId shutting down mappers")
      prevMappers.values.foreach(_.stopStream)
      if(Configuration.Vehicles.shutdownReducersWhenComplete) {
        shutdownReducers
      }
    }

    override def fullShutdown: Unit = {
      shutdownReducers
    }
  }

  private lazy val temporalDaemons = {
    logger.info(s"$nodeId initializing temporal daemons")
    Seq(SensorDaemon, MapReduceDaemon, ClusterMembershipDaemon)
  }

  private def executeInterval(currentSimTime: Timestamp) {
    logger.info(s"$nodeId executing for $currentSimTime: $temporalDaemons")
    temporalDaemons.foreach(_.executeInterval(currentSimTime))
  }

  private def tick(timings: Seq[TimeMapping], replyWhenDone: Option[ActorRef]) {
    executeInterval(timings.head.simTime)
    val remainingTimings = timings.tail
    remainingTimings.headOption match {
      case None => {
        logger.info(s"Vehicle $nodeId has finished at $currentTime")
        replyWhenDone match {
          case Some(receiver) => {
            logger.info(s"$nodeId sending DONE to $receiver")
            receiver ! Vehicle.SimulationDone(nodeId)
          }
          case None => logger.debug(s"$nodeId done (no replyWhenDone actor specified)")
        }
        logger.info(s"Vehicle $nodeId performing safe shutdown")
        temporalDaemons.foreach(_.safeShutdown)
        logger.debug(s"Vehicle $nodeId completed safe shutdown")
      }
      case Some(nextTime: TimeMapping) => {
        sleepUntil(nextTime.wallTime)
        tick(remainingTimings, replyWhenDone)
      }
    }
  }

  protected def startSimulation(startTime: Timestamp, replyWhenDone: Option[ActorRef]) {
    logger.info(s"$nodeId will start at epoch $startTime")
    val timings = mkTimings(startTime)
    logger.debug(s"$nodeId generated timings ${timings(0)} to ${timings.last}")
    sleepUntil(timings.head.wallTime)
    logger.info(s"Simulation running")
    tick(timings, replyWhenDone)
  }

  override def preStart {
    logger.debug(s"$nodeId running pre-start")
    if (warmCaches) {
      // Force evaluation of lazy properties
      val props = fullProperties
      logger.debug(s"$nodeId props: $props")
      val cacheTypes = this.caches.keys
      logger.debug(s"$nodeId cache types $cacheTypes")
      val numDaemons = temporalDaemons.size
      logger.debug(s"$nodeId has $numDaemons temporal daemons")
      logger.debug(s"Created cluster head connection ${this.clusterHeadConnection.actorRef}")
      logger.debug(s"Created middleware connection ${this.middlewareConnection}")
    }
    logger.debug(s"$nodeId finished pre-start")
  }

  def receive: PartialFunction[Any, Unit] = {
    case Vehicle.StartMessage(startTime, replyWhenDone) => {
      logger.info(s"Node $nodeId got start message")
      sender ! Vehicle.StartingMessage(nodeId)
      startSimulation(startTime, replyWhenDone)
    }
    case Ping() => {
      logger.debug("Replying to ping")
      sender ! Ping()
    }
    case Vehicle.CheckReadyMessage => {
      logger.info(s"$nodeId got checkReady")
      sender ! Vehicle.ReadyMessage(nodeId)
    }
    case Terminated(clusterHeadConnection) => {
      logger.error(s"Cluster head connection crashed for $nodeId")
      throw new RuntimeException(s"$nodeId clusterhead conn crashed")
    }
    case Vehicle.FullShutdown() => {
      logger.info(s"Vehicle $nodeId is fully shutting down")
      this.temporalDaemons.foreach(_.fullShutdown)
    }
    case _ => throw new RuntimeException(s"Unknown message on $nodeId")
  }
}