package edu.rpi.cs.nsl.spindle.vehicle.simulation

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.typeTag

import org.slf4j.LoggerFactory

import akka.actor.Actor
import akka.actor.ActorLogging
import edu.rpi.cs.nsl.spindle.datatypes.{ Vehicle => VehicleMessage }
import edu.rpi.cs.nsl.spindle.datatypes.VehicleColors
import edu.rpi.cs.nsl.spindle.datatypes.VehicleTypes
import edu.rpi.cs.nsl.spindle.vehicle.Types.NodeId
import edu.rpi.cs.nsl.spindle.vehicle.Types.Timestamp
import edu.rpi.cs.nsl.spindle.vehicle.kafka.ClientFactory
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.PositionCache
import edu.rpi.cs.nsl.spindle.vehicle.simulation.sensors.MockSensor
import scala.reflect.runtime.universe._
import akka.actor.Props

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

object Vehicle {
  case class StartMessage(startTime: Double)
  def props(nodeId: NodeId,
            clientFactory: ClientFactory,
            timestamps: Seq[Timestamp],
            positions: PositionCache,
            mockSensors: Set[MockSensor[Any]],
            properties: Set[TypedValue[Any]]) = {
    Props(new Vehicle(nodeId: NodeId,
      clientFactory: ClientFactory,
      timestamps: Seq[Timestamp],
      positions: PositionCache,
      mockSensors: Set[MockSensor[Any]],
      properties: Set[TypedValue[Any]]))
  }
}

/**
 * Simulates an individual vehicle
 */
class Vehicle(nodeId: NodeId,
              clientFactory: ClientFactory,
              timestamps: Seq[Timestamp],
              positions: PositionCache,
              mockSensors: Set[MockSensor[Any]],
              properties: Set[TypedValue[Any]])
    extends Actor with ActorLogging { //TODO: kafka references
  private val logger = LoggerFactory.getLogger(s"${this.getClass.getName}-$nodeId")
  private lazy val fullProperties: Iterable[TypedValue[Any]] = properties.toSeq ++
    Seq(TypedValue[VehicleTypes.VehicleId](nodeId)).asInstanceOf[Seq[TypedValue[Any]]]
  private[simulation] def generateMessage(timestamp: Timestamp): VehicleMessage = {
    import VehicleTypes._
    val readings: Seq[TypedValue[Any]] = ({
      val position = positions.getPosition(timestamp)
      val mph = TypedValue[MPH](position.speed)
      val lat = TypedValue[Lat](position.x)
      val lon = TypedValue[Lon](position.y)
      mockSensors.toSeq.map(_.getReading(timestamp)) ++ Seq[TypedValue[_]](mph, lat, lon)
    }).asInstanceOf[Seq[TypedValue[Any]]]
    VehicleMessageFactory.mkVehicle(readings, fullProperties)
  }
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
    logger.trace(s"$nodeId generated timings $timings")
    //TODO
    throw new RuntimeException("Not implemented")
  }

  def receive = {
    case Vehicle.StartMessage(startTime) => startSimulation(startTime)
  }

}

//TODO: static vehicle (one cluster per vehicle)
//TODO: database vehicle (interface with postgres)