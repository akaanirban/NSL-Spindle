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
import edu.rpi.cs.nsl.spindle.vehicle.TypedValue
import edu.rpi.cs.nsl.spindle.vehicle.ReflectionUtils

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