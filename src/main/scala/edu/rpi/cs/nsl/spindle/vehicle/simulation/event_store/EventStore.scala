package edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store

trait EventStore {
  def close
  def getReadings(nodeId: Int): Stream[TSEntry]
}