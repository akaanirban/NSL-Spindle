package edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store

import java.sql.ResultSet
import java.sql.Connection
import edu.rpi.cs.nsl.spindle.vehicle.Types._
import scala.concurrent.duration._
import edu.rpi.cs.nsl.spindle.vehicle.simulation.Configuration.Vehicles.eventsPerSecondMod

case class PositionEntry(timestamp: FiniteDuration, x: Double, y: Double, speed: Double) extends TSEntry[Position](timestamp) {
  def getReading: Position = Position(x, y, speed)
}

class PositionIterator(resultSet: ResultSet) extends QueryIterator[TSEntry[Position]](resultSet) { //TODO: rename to positionIterator
  def next: TSEntry[Position] = {
    PositionEntry(resultSet.getDouble("timestamp") seconds,
      resultSet.getDouble("x"),
      resultSet.getDouble("y"),
      resultSet.getDouble("speed"))
  }
}

class PositionQuery(connection: Connection) extends {
  //NOTE: we mod timestamp by 1 to get data only once per second
  //scalastyle:off whitespace.end.of.line
  private val statement = s"""SELECT 
      x.timestamp as timestamp, 
      x.reading as x, 
      y.reading as y, 
      s.reading as speed
    FROM posx x, posy y, speed s
    WHERE (x.timestamp = y.timestamp and y.timestamp = s.timestamp)
      and x.timestamp % $eventsPerSecondMod = 0
      and (x.node = y.node and y.node = s.node) 
      and x.node = ?
    ORDER BY x.timestamp"""
  //scalastyle:on whitespace.end.of.line
} with JdbcQuery(connection, statement) {
  def loadReadings(nodeId: NodeId): Stream[TSEntry[Position]] = {
    setNode(nodeId)
    val positionStream = new PositionIterator(executeQuery).toStream
    assert(positionStream.headOption.isDefined, s"No position information for node $nodeId")
    positionStream
    //TODO: ensure this doesn't cause memory leak (probably best to return iterator and convert to stream in calling method)
  }
}