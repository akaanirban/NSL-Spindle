package edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store

import edu.rpi.cs.nsl.spindle.vehicle.Types._

case class Position(x: Double, y: Double, speed: Double)

object CacheTypes extends Enumeration {
  val PositionCache = Value
}
import CacheTypes._

class TSCache[T](readings: Iterable[TSEntry], mapper: (TSEntry) => T) {
  private val cache: Map[Timestamp, T] = readings
    .map { reading =>
      (reading.timestamp, mapper(reading))
    }
    .toMap

  def getReading(timestamp: Timestamp): T = cache(timestamp)
  def getTimestamps: Iterable[Timestamp] = cache.keys
}
