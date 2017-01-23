package edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store

import java.sql.ResultSet
import java.sql.Connection
import scala.concurrent.duration._

/**
 * Contains database time-series data
 */
case class TSEntry(timestamp: Double, x: Double, y: Double, speed: Double) {
  def toPosition: Position = Position(x, y, speed)
  def getTimestamp: Long = {
    (timestamp seconds).toMillis
  }
}

class TimeSeriesIterator(resultSet: ResultSet) extends QueryIterator[TSEntry](resultSet) {
  def next: TSEntry = {
    TSEntry(resultSet.getDouble("timestamp"),
      resultSet.getDouble("x"),
      resultSet.getDouble("y"),
      resultSet.getDouble("speed"))
  }
}

class TimeSeriesQuery(connection: Connection) extends {
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
  def loadReadings(nodeId: Int): Stream[TSEntry] = {
    setNode(nodeId)
    new TimeSeriesIterator(executeQuery).toStream
    //TODO: ensure this doesn't cause memory leak (probably best to return iterator and convert to stream in calling method)
  }
}