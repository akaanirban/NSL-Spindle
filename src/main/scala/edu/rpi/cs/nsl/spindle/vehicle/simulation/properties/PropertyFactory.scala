package edu.rpi.cs.nsl.spindle.vehicle.simulation.properties

import scala.reflect.runtime.universe
import scala.util.Random

import edu.rpi.cs.nsl.spindle.vehicle.Types.NodeId
import edu.rpi.cs.nsl.spindle.vehicle.simulation.TypedValue

trait PropertyFactory {
  def getProperties(nodeId: NodeId): Set[TypedValue[Any]]
}

//TODO: get properties from database, json, xml

/**
 * Get properties that are explicitly defined
 * in the Vehicle case class (to be renamed VehicleStatus)
 */
class BasicPropertyFactory extends PropertyFactory {
  import edu.rpi.cs.nsl.spindle.datatypes._
  private val rand = new Random()
  private lazy val colors = VehicleColors.values.toArray
  private def getColor: VehicleColors.Value = {
    val colorIndex = rand.nextInt(colors.length)
    colors(colorIndex)
  }
  def getProperties(nodeId: NodeId) = {
    val color = TypedValue[VehicleColors.Value](getColor).asInstanceOf[TypedValue[Any]]
    Set(color)
  }
}