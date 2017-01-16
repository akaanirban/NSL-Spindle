package edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store

import CacheTypes._

class CacheFactory(store: EventStore) {
  def mkCaches(nodeId: Int) = {
    val entries = store.getReadings(nodeId).force.toList
    val timestamps = entries.map(_.timestamp)
    val positionCache = new TSCache[Position](entries, _.toPosition)
    (timestamps, Map(PositionCache -> positionCache))
  }
}