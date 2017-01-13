package edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store

import edu.rpi.cs.nsl.spindle.vehicle.Types._

case class Position(x: Double, y: Double, speed: Double)

class TSCache[T](readings: Iterable[TSEntry], mapper: (TSEntry) => T) {
  private val cache: Map[Timestamp, T] = readings
    .map { reading =>
      (reading.timestamp, mapper(reading))
    }
    .toMap

  def getReading(timestamp: Timestamp): T = cache(timestamp)
  def getTimestamps: Iterable[Timestamp] = cache.keys
}

object CacheTypes extends Enumeration {
  val PositionCache = Value
}
import CacheTypes._

class CacheFactory(store: EventStore) {
  def mkCaches(nodeId: Int) = {
    val entries = store.getReadings(nodeId).force.toList
    val timestamps = entries.map(_.timestamp)
    val positionCache = new TSCache[Position](entries, _.toPosition)
    (timestamps, Map(PositionCache -> positionCache))
  }
}