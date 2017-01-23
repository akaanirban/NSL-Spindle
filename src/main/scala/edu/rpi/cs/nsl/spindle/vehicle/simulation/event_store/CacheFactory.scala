package edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store

import edu.rpi.cs.nsl.spindle.vehicle.Types._
import CacheTypes._

class CacheFactory(store: EventStore) {
  def mkCaches(nodeId: Int): (Seq[Timestamp], Map[CacheTypes.Value, TSEntryCache[_]]) = {
    val entries = store.getReadings(nodeId).force.toList
    val timestamps: Seq[Timestamp] = entries.map(_.getTimestamp)
    val positionCache = new TSEntryCache[Position](entries, _.toPosition)
    //TODO: cluster-head cache
    (timestamps, Map(PositionCache -> positionCache))
  }
}