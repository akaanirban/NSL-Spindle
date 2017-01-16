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
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.TSCache
import edu.rpi.cs.nsl.spindle.vehicle.simulation.sensors.MockSensor
import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.TopicLookupService

/**
 * Wraps a value to prevent type erasure
 */
case class TypedValue[T: TypeTag](value: T) {
  def getTypeString: String = typeTag[T].toString
}

object ReflectionUtils {
  def getTypeString[T: TypeTag] = typeTag[T].toString
  def getMatchingTypes(collection: Iterable[TypedValue[Any]], typeString: String) = {
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
    matches.last.value.asInstanceOf[T]
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
  case class StartMessage(startTime: Double)
  case class ReadyMessage(nodeId: NodeId)
  // Sent by world
  case class CheckReadyMessage()
  def props(nodeId: NodeId,
            clientFactory: ClientFactory,
            cacheFactory: CacheFactory,
            mockSensors: Set[MockSensor[Any]],
            properties: Set[TypedValue[Any]],
            warmCaches: Boolean = true) = {
    Props(new Vehicle(nodeId: NodeId,
      clientFactory: ClientFactory,
      cacheFactory: CacheFactory,
      mockSensors: Set[MockSensor[Any]],
      properties: Set[TypedValue[Any]],
      warmCaches))
  }
}

/**
 * Simulates an individual vehicle
 *
 * @todo - Mappers, Reducers
 */
class Vehicle(nodeId: NodeId,
              clientFactory: ClientFactory,
              cacheFactory: CacheFactory,
              mockSensors: Set[MockSensor[Any]],
              properties: Set[TypedValue[Any]],
              // Disable cache warming for faster tests
              warmCaches: Boolean = true)
    extends Actor with ActorLogging { //TODO: kafka references
  private lazy val logger = Logging(context.system, this)
  private lazy val fullProperties: Iterable[TypedValue[Any]] = properties.toSeq ++
    Seq(TypedValue[VehicleTypes.VehicleId](nodeId)).asInstanceOf[Seq[TypedValue[Any]]]
  private lazy val (timestamps, caches): (Seq[Timestamp], Map[CacheTypes.Value, TSCache[_]]) = cacheFactory.mkCaches(nodeId)
  private lazy val statusProducer = {
    logger.debug("Creating status producer")
    clientFactory.mkProducer[NodeId, VehicleMessage](TopicLookupService.getVehicleStatus(nodeId))
  }

  /**
   * Create a Vehicle status message
   */
  private[simulation] def generateMessage(timestamp: Timestamp): VehicleMessage = {
    import VehicleTypes._
    import CacheTypes._
    val readings: Seq[TypedValue[Any]] = ({
      val position: Position = caches(PositionCache)
        .asInstanceOf[TSCache[Position]]
        .getReading(timestamp)
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
  private[simulation] def mkTimings(startTime: Double): Seq[Double] = {
    val zeroTime = timestamps.min
    val offsets = timestamps.map(_ - zeroTime)
    val absoluteTimes = offsets.map(_ + startTime)
    // Ensure ascending order
    absoluteTimes.sorted.reverse
  }
  private def startSimulation(startTime: Double) {
    logger.info(s"$nodeId will start at epoch $startTime")
    val timings = mkTimings(startTime)
    logger.debug(s"$nodeId generated timings $timings")
    //TODO
    throw new RuntimeException("Not implemented")
  }

  override def preStart {
    if (warmCaches) {
      // Force evaluation
      val props = fullProperties
      val cacheTypes = this.caches.keys
      logger.debug(s"Cache types $cacheTypes")
    }
  }

  def receive = {
    case Vehicle.StartMessage(startTime) => startSimulation(startTime)
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

//TODO: static vehicle (one cluster per vehicle)
//TODO: database vehicle (interface with postgres)