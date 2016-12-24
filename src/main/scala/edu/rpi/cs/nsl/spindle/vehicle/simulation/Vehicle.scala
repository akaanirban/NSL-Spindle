package edu.rpi.cs.nsl.spindle.vehicle.simulation

import edu.rpi.cs.nsl.spindle.datatypes.VehicleTypes
import edu.rpi.cs.nsl.spindle.vehicle.data_sources.DataSource
import scala.reflect.runtime.universe._

/**
 * Simulates an individual vehicle
 */
abstract class Vehicle(id: String, config: Configuration) {
  protected val sources: Map[String, DataSource[_]] = {
    val factory = new SourceFactory(config)
    def mkName(name: String) = s"simulator-$id-$name"
    Map("mph" -> factory.mkSource[VehicleTypes.MPH](mkName("mph")),
      "lat" -> factory.mkSource[VehicleTypes.Lat](mkName("lat")),
      "lon" -> factory.mkSource[VehicleTypes.Lon](mkName("lon")))
  }

  protected def getReading[T](name: String): T

  /**
   * Produce data for a particular epoch
   */
  def mkReadings(epoch: String) {
    sources.foreach {
      case ((key, dataSource)) =>
        dataSource.send(getReading(key))
    }
  }
}

//TODO: static vehicle (one cluster per vehicle)
//TODO: database vehicle (interface with postgres)