package edu.rpi.cs.nsl.spindle.vehicle.simulation.sensors

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import edu.rpi.cs.nsl.spindle.vehicle.Types.Timestamp
import scala.util.Random
import edu.rpi.cs.nsl.spindle.vehicle.simulation.TypedValue

/**
 * Use generator function to make readings and cast them to specified type
 */
private[simulation] sealed abstract class MockDoubleSensor[T: TypeTag: ClassTag](name: String, generator: (Timestamp) => Double) extends MockSensor[T](name) {
  def getReading(timestamp: Timestamp): TypedValue[T] = {
    TypedValue[T](generator(timestamp).asInstanceOf[T])
  }
}

private[simulation] sealed class SingleValueSensor[T: TypeTag: ClassTag](name: String, value: Double) extends {
  private val generator = (timestamp: Timestamp) => value
} with MockDoubleSensor[T](name, generator)

private[simulation] sealed class RngValueSensor[T: TypeTag: ClassTag](name: String, maxValue: Double = 1.0) extends {
  private val rng = new Random()
  private val generator = (timestamp: Timestamp) => (rng.nextGaussian * maxValue)
} with MockDoubleSensor[T](name, generator)