package edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store

import edu.rpi.cs.nsl.spindle.vehicle.Types._

case class Position(x: Double, y: Double, speed: Double)

object CacheTypes extends Enumeration {
  val PositionCache, ClusterCache = Value
}
import CacheTypes._
import scala.concurrent.duration.FiniteDuration

trait TSCache[T] {
  val cache: Map[Timestamp, T] //TODO: protected
  def getValueOpt(currentSimTime: Timestamp): Option[T] = cache.get(currentSimTime)
  def getOrPriorOpt(currentSimTime: Timestamp): Option[T] = cache.getOrPriorOpt(currentSimTime)
  def getTimestamps: Iterable[Timestamp] = cache.keys

  /**
   * Map from timestamp to value that can return the closest prior value
   */
  protected implicit class TSMap[T](map: Map[Timestamp, T]) {
    def getOrPriorOpt(timestamp: Timestamp): Option[T] = {
      val priorTimes = map.keys.filter(_ <= timestamp)
      if (priorTimes.isEmpty) {
        None
      } else {
        val nearestTime = priorTimes.max
        Some(map(nearestTime))
      }
    }
  }
}

/**
 * Contains database time-series data
 *
 * @param timestamp - database timestamp (in seconds)
 */
abstract class TSEntry[T](timestamp: FiniteDuration) {
  // Get timestamp in milliseconds
  def getTimestamp: Long = {
    timestamp.toMillis
  }
  def getReading: T
}

/**
 * @note - can be used to store mock sensor data?
 *
 * @todo - clean this up
 */
class TSEntryCache[T](readings: Iterable[TSEntry[T]]) extends TSCache[T] {
  val cache: Map[Timestamp, T] = readings
    .map { reading =>
      (reading.getTimestamp -> reading.getReading)
    }
    .toMap
}

