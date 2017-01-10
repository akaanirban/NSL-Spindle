package edu.rpi.cs.nsl.spindle.vehicle.simulation.sensors

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import edu.rpi.cs.nsl.spindle.vehicle.Types.Timestamp
import edu.rpi.cs.nsl.spindle.vehicle.simulation.TypedValue

/**
 * Top Level class for simulated sensors
 */
private[simulation] abstract class MockSensor[T: TypeTag: ClassTag](name: String) { //TODO: type tags
  /**
   * Produce simulated reading for specified time stamp
   */
  def getReading(timestamp: Timestamp): TypedValue[T]
}

