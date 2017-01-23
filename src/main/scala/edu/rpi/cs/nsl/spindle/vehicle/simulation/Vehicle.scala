package edu.rpi.cs.nsl.spindle.vehicle.simulation

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
import edu.rpi.cs.nsl.spindle.datatypes.{ Vehicle => VehicleMessage }
import edu.rpi.cs.nsl.spindle.datatypes.VehicleColors
import edu.rpi.cs.nsl.spindle.datatypes.VehicleTypes
import edu.rpi.cs.nsl.spindle.vehicle.Types._
import edu.rpi.cs.nsl.spindle.vehicle.kafka.ClientFactory
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.CacheFactory
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.CacheTypes
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.CacheTypes
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.Position
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.TSEntryCache
import edu.rpi.cs.nsl.spindle.vehicle.simulation.sensors.MockSensor
import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.TopicLookupService
import edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations.TransformationStore
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext
import edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations.ActiveTransformations
import edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations.MapperFunc
import edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations.KvReducerFunc
import edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations.TransformationFunc
import edu.rpi.cs.nsl.spindle.vehicle.kafka.streams.StreamExecutor
import java.util.concurrent.Executors

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
  def mkVehicle(readings: Iterable[TypedValue[Any]],
                properties: Iterable[TypedValue[Any]]): VehicleMessage = {
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
            cacheFactory: CacheFactory,
            mockSensors: Set[MockSensor[Any]],
            properties: Set[TypedValue[Any]],
            warmCaches: Boolean = true): Props = {
    Props(new Vehicle(nodeId: NodeId,
      clientFactory: ClientFactory,
      transformationStore: TransformationStore,
      cacheFactory: CacheFactory,
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
 * @todo - Mappers, Reducers
 */
class Vehicle(nodeId: NodeId,
              clientFactory: ClientFactory,
              transformationStore: TransformationStore,
              cacheFactory: CacheFactory,
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
  private lazy val (timestamps, caches): (Seq[Timestamp], Map[CacheTypes.Value, TSEntryCache[_]]) = cacheFactory.mkCaches(nodeId)

  /**
   * Create a Vehicle status message
   */
  private[simulation] def generateMessage(timestamp: Timestamp): VehicleMessage = {
    import VehicleTypes._
    import CacheTypes._
    val readings: Seq[TypedValue[Any]] = ({
      val position: Position = caches(PositionCache)
        .asInstanceOf[TSEntryCache[Position]]
        .getValue(timestamp)
      val mph = TypedValue[MPH](position.speed)
      val lat = TypedValue[Lat](position.x)
      val lon = TypedValue[Lon](position.y)
      mockSensors.toSeq.map(_.getReading(timestamp)) ++ Seq[TypedValue[_]](mph, lat, lon)
    }).asInstanceOf[Seq[TypedValue[Any]]]
    VehicleMessageFactory.mkVehicle(readings, fullProperties)
  }

  /**
   * Create mapping from simulator time to future epoch times based on startTime
   */
  private[simulation] def mkTimings(startTime: Timestamp): Seq[Timestamp] = {
    val zeroTime = timestamps.min
    val offsets = timestamps.map(_ - zeroTime)
    val absoluteTimes = offsets.map(_ + startTime)
    // Ensure ascending order
    absoluteTimes.sorted
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
    def executeInterval(currentTiming: Timestamp): Unit
  }

  private object SensorDaemon extends TemporalDaemon {
    private lazy val outTopic = TopicLookupService.getVehicleStatus(nodeId)
    private val producer = clientFactory.mkProducer[NodeId, VehicleMessage](outTopic)
    def executeInterval(currentTiming: Timestamp) {
      val message = generateMessage(currentTiming)
      producer.send(nodeId, message)
      logger.debug(s"$nodeId sent status message")
    }
  }

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
      toRemove.map(prevMap(_)).foreach(_.stop)
      val newTransformers = toAdd.map(func => (func -> func.getTransformExecutor(clientFactory))).toMap
      newTransformers.values.foreach(pool.execute(_))
      (prevMap -- toRemove) ++ newTransformers
    }
    private def updateMappers(mappers: Iterable[MapperFunc[Any, Any, Any, Any]]) {
      val nextMap = updateTransformers(mappers, prevMappers)
      assert(nextMap.keySet equals mappers.toSet)
      prevMappers = nextMap
    }
    private def updateReducers(reducers: Iterable[KvReducerFunc[Any, Any]]) {
      prevReducers = updateTransformers(reducers, prevReducers)
    }
    def executeInterval(currentTiming: Timestamp) {
      val ActiveTransformations(mappers, reducers) = transformationStore
        .getActiveTransformations(currentTiming)
      updateMappers(mappers)
      updateReducers(reducers)
      logger.info(s"$nodeId updated mappers and reducers")
    }
  }

  private lazy val temporalDaemons = Seq(SensorDaemon, MapReduceDaemon)

  private def executeInterval(currentTiming: Timestamp) {
    logger.info(s"$nodeId executing for $currentTiming")
    temporalDaemons.foreach(_.executeInterval(currentTiming))
  }

  private def tick(timings: Seq[Timestamp], replyWhenDone: Option[ActorRef]) {
    executeInterval(timings.head)
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
      case Some(epochTime: Timestamp) => {
        sleepUntil(epochTime)
        tick(remainingTimings, replyWhenDone)
      }
    }
  }

  protected def startSimulation(startTime: Timestamp, replyWhenDone: Option[ActorRef]) {
    logger.info(s"$nodeId will start at epoch $startTime")
    val timings = mkTimings(startTime)
    logger.debug(s"$nodeId generated timings ${timings(0)} to ${timings.last}")
    sleepUntil(timings.head)
    logger.info(s"Simulation running")
    tick(timings.tail, replyWhenDone)
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
    case _ => throw new RuntimeException(s"Unknown message on $nodeId")
  }
}