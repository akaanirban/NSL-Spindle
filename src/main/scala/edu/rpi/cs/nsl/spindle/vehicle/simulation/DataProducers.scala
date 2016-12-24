package edu.rpi.cs.nsl.spindle.vehicle.simulation

import edu.rpi.cs.nsl.spindle.vehicle.data_sources.DataProducer
import edu.rpi.cs.nsl.spindle.vehicle.data_sources.DataSource

class SourceFactory(config: Configuration) {
  def mkSource[T](name: String) = {
    val producer = new DataProducer[T](servers = config.kafkaServers)
    new DataSource[T](name, producer)
  }
}