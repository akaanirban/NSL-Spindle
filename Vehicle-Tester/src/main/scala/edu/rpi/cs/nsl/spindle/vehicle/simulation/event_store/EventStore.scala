package edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store

import edu.rpi.cs.nsl.spindle.vehicle.Types._

trait EventStore {
  def close
  def mkCaches(nodeId: Int): (Seq[Timestamp], CacheMap)
  def getNodes: Iterable[NodeId]
  def getMinSimTime: Timestamp
}
