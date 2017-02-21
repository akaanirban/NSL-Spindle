package edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store

import scala.concurrent.duration._
import java.sql.Connection

import edu.rpi.cs.nsl.spindle.vehicle.simulation.Configuration
import edu.rpi.cs.nsl.spindle.vehicle.simulation.Configuration.Vehicles.eventsPerSecondMod

case class TimeRange(minTime: FiniteDuration, maxTime: FiniteDuration)
/**
 * Get range of sim-world times where vehicles have valid position information
 */
class TimeRangeQuery(connection: Connection) extends {
  private val statement = s"""SELECT 
      min(timestamp) min_time, max(timestamp) as max_time
    FROM ${Configuration.Vehicles.nodePositionsTable}
    WHERE timestamp % $eventsPerSecondMod = 0"""
} with JdbcQuery(connection, statement) {
  def loadTimeMinMax: TimeRange = {
    val resultSet = executeQuery
    resultSet.next
    val minTime = (resultSet.getDouble("min_time") seconds)
    val maxTime = (resultSet.getDouble("max_time") seconds)
    TimeRange(minTime, maxTime)
  }
}