package edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store

case class Position(x: Double, y: Double, speed: Double)

class PositionCache(nodeId: Int, store: EventStore) {
  type Timestamp = Double
  private val positions: Map[Timestamp, Position] = store
    .getReadings(nodeId)
    .force
    .map { reading =>
      (reading.timestamp, reading.toPosition)
    }
    .toMap

  def getPosition(timestamp: Double) = positions(timestamp)
  def getTimestamps = positions.keys
}
