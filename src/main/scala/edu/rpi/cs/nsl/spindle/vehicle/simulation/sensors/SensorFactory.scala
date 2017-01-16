package edu.rpi.cs.nsl.spindle.vehicle.simulation.sensors

import edu.rpi.cs.nsl.spindle.vehicle.Types.NodeId
import edu.rpi.cs.nsl.spindle.vehicle.simulation.Configuration

object SensorFactory {
  import Configuration.Vehicles.Sensors
  private type AnySensor = MockSensor[Any]
  def mkSensors(nodeId: NodeId): Set[MockSensor[Any]] = {
    val valSensors = Sensors.singleValSensors.map {
      case (name, value) =>
        new SingleValueSensor[Double](name, value).asInstanceOf[AnySensor]
    }
    val rngSensors = Sensors.rngSensors.map {
      case (name, maxValue) =>
        new RngValueSensor[Double](name, maxValue).asInstanceOf[AnySensor]
    }

    (valSensors ++ rngSensors).toSet
  }
}