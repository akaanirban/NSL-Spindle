package edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store

trait EventStore {
  type ReadingStream = Stream[Reading]
  def close
  def getXPositions(nodeId: Int): ReadingStream
  def getYPositions(nodeId: Int): ReadingStream
  def getSpeeds(nodeId: Int): ReadingStream
}