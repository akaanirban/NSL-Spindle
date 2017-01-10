package edu.rpi.cs.nsl.spindle.vehicle.simulation

import org.slf4j.LoggerFactory

import edu.rpi.cs.nsl.spindle.vehicle.Types.NodeId
import edu.rpi.cs.nsl.spindle.vehicle.kafka.ClientFactory
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.PositionCache
import edu.rpi.cs.nsl.spindle.datatypes.VehicleColors
import edu.rpi.cs.nsl.spindle.vehicle.simulation.sensors.MockSensor
import edu.rpi.cs.nsl.spindle.datatypes.VehicleTypes
import edu.rpi.cs.nsl.spindle.vehicle.Types.Timestamp
import edu.rpi.cs.nsl.spindle.datatypes.{ Vehicle => VehicleMessage }
import scala.reflect.runtime.universe._

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
    throw new RuntimeException("Not implemented") //TODO: extract to constructor args for vehicle
  }
}
object VehicleMessageFactory extends VehicleMessageFactory {}

/**
 * Simulates an individual vehicle
 */
class Vehicle(nodeId: NodeId, clientFactory: ClientFactory, positions: PositionCache, mockSensors: Set[MockSensor[Any]], properties: Set[TypedValue[Any]]) extends Runnable { //TODO: starting time, kafka references
  private val logger = LoggerFactory.getLogger(s"${this.getClass.getName}-$nodeId")
  private val fullProperties: Iterable[TypedValue[Any]] = properties.toSeq ++ Seq(TypedValue[VehicleTypes.VehicleId](nodeId)).asInstanceOf[Seq[TypedValue[Any]]]
  private def generateMessage(timestamp: Timestamp): VehicleMessage = {
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
  def run {
    logger.info(s"$nodeId starting")
    //TODO
    throw new RuntimeException("Not implemented")
  }
}

//TODO: static vehicle (one cluster per vehicle)
//TODO: database vehicle (interface with postgres)