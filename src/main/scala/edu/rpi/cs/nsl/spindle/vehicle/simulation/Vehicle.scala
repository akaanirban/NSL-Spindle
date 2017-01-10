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
import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration.Duration

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

case class TimingConfig(timestamps: Seq[Timestamp], startTime: Future[Double])

/**
 * Simulates an individual vehicle
 */
class Vehicle(nodeId: NodeId,
              clientFactory: ClientFactory,
              timing: TimingConfig,
              positions: PositionCache,
              mockSensors: Set[MockSensor[Any]],
              properties: Set[TypedValue[Any]])
    extends Runnable { //TODO: starting time, kafka references
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
    import timing.timestamps
    val zeroTime = timestamps.min
    val offsets = timestamps.map(_ - zeroTime)
    val absoluteTimes = offsets.map(_ + startTime)
    // Ensure ascending order
    absoluteTimes.sorted.reverse
  }
  def run {
    logger.info(s"$nodeId starting and waiting for startTime")
    val startTime = Await.result(timing.startTime, Duration.Inf)
    logger.info(s"$nodeId will start at epoch $startTime")
    val timings = mkTimings(startTime)
    logger.trace(s"$nodeId generated timings $timings")
    //TODO
    throw new RuntimeException("Not implemented")
  }
}

//TODO: static vehicle (one cluster per vehicle)
//TODO: database vehicle (interface with postgres)