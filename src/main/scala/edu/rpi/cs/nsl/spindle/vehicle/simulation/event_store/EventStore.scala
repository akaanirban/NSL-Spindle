package edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store

trait EventStore {
  type NodeId = Int
  def close
  def getReadings(nodeId: Int): Stream[TSEntry]
  def getNodes: Stream[NodeId]
}