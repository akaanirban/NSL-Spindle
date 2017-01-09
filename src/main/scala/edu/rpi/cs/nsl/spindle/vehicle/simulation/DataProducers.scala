package edu.rpi.cs.nsl.spindle.vehicle.simulation

import edu.rpi.cs.nsl.spindle.vehicle.data_sources.DataProducer
import edu.rpi.cs.nsl.spindle.vehicle.data_sources.DataSource

object SourceFactory {
  def mkSource[T](name: String) = {
    val producer = new DataProducer[T](servers = Configuration.kafkaServers)
    new DataSource[T](name, producer)
  }
}