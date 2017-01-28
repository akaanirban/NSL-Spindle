package edu.rpi.cs.nsl.spindle.vehicle.simulation

import java.util.concurrent.TimeUnit

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._
import scala.reflect.runtime.universe.typeTag

import org.slf4j.LoggerFactory

import akka.actor._
import akka.actor.Actor._
import akka.actor.Props
import akka.dispatch.BoundedMessageQueueSemantics
import akka.dispatch.RequiresMessageQueue
import akka.event.Logging
import akka.pattern.ask
import edu.rpi.cs.nsl.spindle.datatypes.{ Vehicle => VehicleMessage }
import edu.rpi.cs.nsl.spindle.datatypes.VehicleColors
import edu.rpi.cs.nsl.spindle.datatypes.VehicleTypes
import scala.concurrent.duration._
import edu.rpi.cs.nsl.spindle.vehicle.Types._
import edu.rpi.cs.nsl.spindle.vehicle.kafka.ClientFactory
import edu.rpi.cs.nsl.spindle.vehicle.kafka.streams.StreamExecutor
import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.TopicLookupService
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.CacheTypes
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.CacheTypes
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.EventStore
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.Position
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.TSCache
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.TSEntryCache
import edu.rpi.cs.nsl.spindle.vehicle.simulation.sensors.MockSensor
import edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations.ActiveTransformations
import edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations.KvReducerFunc
import edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations.MapperFunc
import edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations.TransformationFunc
import edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations.TransformationStore
import akka.util.Timeout

/**
 * Wraps a value to prevent type erasure
 */
case class TypedValue[T: TypeTag](value: T) {
  def getTypeString: String = typeTag[T].toString
}

object ReflectionUtils {
  def getTypeString[T: TypeTag]: String = typeTag[T].toString
  def getMatchingTypes(collection: Iterable[TypedValue[Any]], typeString: String): Iterable[TypedValue[Any]] = {
    collection.filter(_.getTypeString == typeString)
  }
}

/**
 * Use reflection to generate a Vehicle message
 *
 * @note - this breaks encapsulation, but Scala's support for multiple constructors isn't great
 */
trait VehicleMessageFactory {
  private val logger = LoggerFactory.getLogger(this.getClass)
  protected def getValueOfType[T: TypeTag](collection: Iterable[TypedValue[Any]]): T = {
    val typeString = ReflectionUtils.getTypeString[T]
    val matches = ReflectionUtils.getMatchingTypes(collection, typeString)
    if (matches.size != 1) {
      logger.error(s"Found more than one entry for $typeString in $collection")
    }
    matches.lastOption match {
      case Some(entry) => entry.value.asInstanceOf[T]
      case None        => throw new RuntimeException(s"No entry found of type $typeString")
    }
  }
  def mkVehicle(readings: Iterable[TypedValue[Any]], properties: Iterable[TypedValue[Any]]): VehicleMessage = {
    import VehicleTypes._
    val id = getValueOfType[VehicleId](properties)
    val lat = getValueOfType[Lat](readings)
    val lon = getValueOfType[Lon](readings)
    val mph = getValueOfType[MPH](readings)
    val color = getValueOfType[VehicleColors.Value](properties)
    VehicleMessage(id, lat, lon, mph, color)
  }
}
object VehicleMessageFactory extends VehicleMessageFactory {}

case class Ping()

object Vehicle {
  case class StartMessage(startTime: Timestamp, replyWhenDone: Option[ActorRef] = None)
  case class ReadyMessage(nodeId: NodeId)
  case class StartingMessage(nodeId: NodeId, eventTime: Timestamp = System.currentTimeMillis)
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
  private val clusterHeadConnection: ActorRef = {
    logger.debug(s"Node $nodeId creating cluster head connection actor")
    val actorRef: ActorRef = context.actorOf(VehicleConnection.props(nodeId, clientFactory.getConfig), name = s"clusterHeadConnector$nodeId")
    //NOTE: insert monitoring here (once message-based tick is set up)
    actorRef
  }

  /**
   * Create a Vehicle status message
   */
  private[simulation] def generateMessage(currentSimTime: Timestamp): VehicleMessage = {
    import VehicleTypes._
    import CacheTypes._
    val readings: Seq[TypedValue[Any]] = ({
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
    }).asInstanceOf[Seq[TypedValue[Any]]]
    VehicleMessageFactory.mkVehicle(readings, fullProperties)
  }

  private[simulation] case class TimeMapping(wallTime: Timestamp, simTime: Timestamp)

  /**
   * Create mapping from simulator time to future epoch times based on startTime
   */
  private[simulation] def mkTimings(startTime: Timestamp): Seq[TimeMapping] = {
    logger.debug(s"Generating timestamps from start time $startTime using sim times $timestamps")
    val zeroTime = timestamps.min
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
  }

  private object SensorDaemon extends TemporalDaemon {
    private lazy val outTopic = TopicLookupService.getVehicleStatus(nodeId)
    private val producer = clientFactory.mkProducer[NodeId, VehicleMessage](outTopic)
    def executeInterval(currentSimTime: Timestamp) {
      val message = generateMessage(currentSimTime)
      producer.send(nodeId, message)
      logger.debug(s"$nodeId sent status message")
    }
  }

  private object ClusterMembershipDaemon extends TemporalDaemon {
    def executeInterval(currentSimTime: Timestamp) {
      val clusterHead: NodeId = caches(CacheTypes.ClusterCache).asInstanceOf[TSEntryCache[NodeId]].getOrPriorOpt(currentSimTime).get
      logger.info(s"Node $nodeId has cluster head $clusterHead at time $currentSimTime")
      Await.result((clusterHeadConnection ? VehicleConnection.SetClusterHead(clusterHead)), Duration.Inf)
      logger.info(s"Node $nodeId has updated cluster head $clusterHead at time $currentSimTime")
    }
  }

  implicit val connectionActorTimeout = Timeout(10 minutes)//TODO: this should be within 1 second

  private object MapReduceDaemon extends TemporalDaemon {
    private lazy val pool = context.dispatcher //TODO: replace with Executors.newCachedThreadPool?
    private var prevMappers: Map[MapperFunc[Any, Any, Any, Any], StreamExecutor] = Map()
    private var prevReducers: Map[KvReducerFunc[Any, Any], StreamExecutor] = Map()
    private def getChanges[T <: TransformationFunc](existing: Set[T], latest: Set[T]) = {
      val toRemove = existing diff latest
      val toAdd = latest diff existing
      (toRemove, toAdd)
    }
    private def updateTransformers[T <: TransformationFunc](toRun: Iterable[T], prevMap: Map[T, StreamExecutor]): Map[T, StreamExecutor] = {
      val (toRemove, toAdd) = getChanges(prevMap.keySet, toRun.toSet)
      toRemove.map(prevMap(_)).foreach(_.stopStream)
      val newTransformers = toAdd.map(func => (func -> func.getTransformExecutor(clientFactory))).toMap
      newTransformers.values.foreach(pool.execute(_))
      newTransformers.values.foreach(transformer => logger.debug(s"Launched executor $transformer"))
      (prevMap -- toRemove) ++ newTransformers
    }
    private def updateMappers(mappers: Iterable[MapperFunc[Any, Any, Any, Any]]) {
      val nextMap = updateTransformers(mappers, prevMappers)
      assert(nextMap.keySet equals mappers.toSet)
      prevMappers = nextMap
    }
    private def updateReducers(reducers: Iterable[KvReducerFunc[Any, Any]]) {
      val nextReducers = updateTransformers(reducers, prevReducers)
      assert(nextReducers.keySet equals reducers.toSet)
      prevReducers = nextReducers
    }
    def executeInterval(currentSimTime: Timestamp) {
      val ActiveTransformations(mappers, reducers) = transformationStore
        .getActiveTransformations(currentSimTime)
      updateMappers(mappers)
      updateReducers(reducers)
      Await.result((clusterHeadConnection ? VehicleConnection.SetMappers(mappers.map(_.funcId).toSet)), Duration.Inf) //TODO: clean up 
      logger.info(s"$nodeId updated mappers and reducers: $prevMappers $prevReducers")
    }
  }

  private lazy val temporalDaemons = Seq(SensorDaemon, MapReduceDaemon, ClusterMembershipDaemon)

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
    if (warmCaches) {
      // Force evaluation of lazy properties
      val props = fullProperties
      val cacheTypes = this.caches.keys
      val numDaemons = temporalDaemons.size
      logger.debug(s"Cache types $cacheTypes")
    }
  }

  def receive: PartialFunction[Any, Unit] = {
    case Vehicle.StartMessage(startTime, replyWhenDone) => {
      sender ! Vehicle.StartingMessage(nodeId)
      startSimulation(startTime, replyWhenDone)
    }
    case Ping() => {
      logger.debug("Replying to ping")
      sender ! Ping()
    }
    case Vehicle.CheckReadyMessage => {
      logger.info("Got checkReady")
      sender ! Vehicle.ReadyMessage(nodeId)
    }
    case Terminated(clusterHeadConnection) => {
      logger.error(s"Cluster head connection crashed for $nodeId")
      throw new RuntimeException(s"$nodeId clusterhead conn crashed")
    }
    case _ => throw new RuntimeException(s"Unknown message on $nodeId")
  }
}