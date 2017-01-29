package edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store

import scala.concurrent.duration._
import java.sql.Connection
import edu.rpi.cs.nsl.spindle.vehicle.simulation.Configuration.Vehicles.eventsPerSecondMod

case class TimeRange(minTime: FiniteDuration, maxTime: FiniteDuration)
/**
 * Get range of sim-world times where vehicles have valid position information
 */
class TimeRangeQuery(connection: Connection) extends {
  private val statement = s"""SELECT 
      min(x.timestamp) min_time, max(x.timestamp) as max_time
    FROM posx x, posy y, speed s 
    WHERE (x.node = y.node and y.node = s.node) 
      and x.timestamp % $eventsPerSecondMod = 0
      and (x.timestamp = y.timestamp and y.timestamp = s.timestamp)"""
} with JdbcQuery(connection, statement) {
  def loadTimeMinMax: TimeRange = {
    val resultSet = executeQuery
    resultSet.next
    val minTime = (resultSet.getDouble("min_time") seconds)
    val maxTime = (resultSet.getDouble("max_time") seconds)
    TimeRange(minTime, maxTime)
  }
}