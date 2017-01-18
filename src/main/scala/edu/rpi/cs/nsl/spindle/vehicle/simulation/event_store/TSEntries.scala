package edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store

import java.sql.ResultSet
import java.sql.Connection
import scala.concurrent.duration._

/**
 * Contains database time-series data
 */
case class TSEntry(timestamp: Double, x: Double, y: Double, speed: Double) {
  def toPosition = Position(x, y, speed)
  def getTimestamp: Long = {
    (timestamp seconds).toMillis
  }
}

class TimeSeriesIterator(resultSet: ResultSet) extends QueryIterator[TSEntry](resultSet) {
  def next = {
    TSEntry(resultSet.getDouble("timestamp"),
      resultSet.getDouble("x"),
      resultSet.getDouble("y"),
      resultSet.getDouble("speed"))
  }
}

class TimeSeriesQuery(connection: Connection) extends {
  val EVENTS_PER_SECOND = 1
  //NOTE: we mod timestamp by 1 to get data only once per second
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
} with JdbcQuery(connection, statement) {
  def loadReadings(nodeId: Int) = {
    setNode(nodeId)
    new TimeSeriesIterator(executeQuery).toStream
  }
}