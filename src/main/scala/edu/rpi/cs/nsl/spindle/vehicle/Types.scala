package edu.rpi.cs.nsl.spindle.vehicle

import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.CacheTypes
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.TSCache

object Types {
  type NodeId = Int // UID for vehicle in database
  type Timestamp = Long
  type CacheMap = Map[CacheTypes.Value, TSCache[_]]
}