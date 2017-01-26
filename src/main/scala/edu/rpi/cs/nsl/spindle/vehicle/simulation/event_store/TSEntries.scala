package edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store

import java.sql.ResultSet
import java.sql.Connection
import edu.rpi.cs.nsl.spindle.vehicle.Types._
import scala.concurrent.duration._

/**
 * Contains database time-series data
 *
 * @param timesetamp - database timestamp (in seconds)
 */
abstract class TSEntry[T](timestamp: FiniteDuration) {
  // Get timestamp in milliseconds
  def getTimestamp: Long = {
    timestamp.toMillis
  }
  def getReading: T
}

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
  val EVENTS_PER_SECOND = 1
  //NOTE: we mod timestamp by 1 to get data only once per second
  //scalastyle:off whitespace.end.of.line
  private val statement = s"""SELECT 
      x.timestamp as timestamp, 
      x.reading as x, 
      y.reading as y, 
      s.reading as speed
    FROM posx x, posy y, speed s
    WHERE (x.timestamp = y.timestamp and y.timestamp = s.timestamp)
      and x.timestamp % $EVENTS_PER_SECOND = 0
      and (x.node = y.node and y.node = s.node) 
      and x.node = ?
    ORDER BY x.timestamp"""
  //scalastyle:on whitespace.end.of.line
} with JdbcQuery(connection, statement) {
  def loadReadings(nodeId: NodeId): Stream[TSEntry[Position]] = {
    setNode(nodeId)
    new PositionIterator(executeQuery).toStream
    //TODO: ensure this doesn't cause memory leak (probably best to return iterator and convert to stream in calling method)
  }
}