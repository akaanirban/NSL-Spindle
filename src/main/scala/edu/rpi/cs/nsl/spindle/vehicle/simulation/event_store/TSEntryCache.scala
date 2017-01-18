package edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store

import edu.rpi.cs.nsl.spindle.vehicle.Types._

case class Position(x: Double, y: Double, speed: Double)

object CacheTypes extends Enumeration {
  val PositionCache = Value
}
import CacheTypes._

trait TSCache[T] {
  protected val cache: Map[Timestamp, T]
  def getValue(timestamp: Timestamp): T
  def getTimestamps: Iterable[Timestamp]

  /**
   * Map from timestamp to value that can return the closest prior value
   */
  protected implicit class TSMap[T](map: Map[Timestamp, T]) {
    def getOrPrior(timestamp: Timestamp): T = {
      val nearestTime = map.keys.filter(_ <= timestamp).max
      map(nearestTime)
    }
  }
}

class TSEntryCache[T](readings: Iterable[TSEntry], mapper: (TSEntry) => T) extends TSCache[T] {
  protected val cache: Map[Timestamp, T] = readings
    .map { reading =>
      (reading.getTimestamp, mapper(reading))
    }
    .toMap

  def getValue(timestamp: Timestamp): T = cache.getOrPrior(timestamp)
  def getTimestamps: Iterable[Timestamp] = cache.keys
}