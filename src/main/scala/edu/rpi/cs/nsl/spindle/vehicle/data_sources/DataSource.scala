package edu.rpi.cs.nsl.spindle.vehicle.data_sources

/**
 * Pushes sensor data onto Kafka
 */
abstract class DataSource[T](name: String) { // TODO: generate name from type
  def send(reading: T) = {
    
  }
}