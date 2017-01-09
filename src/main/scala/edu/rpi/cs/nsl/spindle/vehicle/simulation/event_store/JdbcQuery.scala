package edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store

import java.sql.ResultSet
import java.sql.Connection

class JdbcQuery(connection: Connection, statement: String) {
  private val preparedStatement = connection.prepareStatement(statement)
  protected def initParameters {} // Can be overridden
  protected def setNode(node: Int) {
    preparedStatement.setInt(1, node)
  }
  protected def getForNode(nodeId: Int): ResultSet = {
    preparedStatement.clearParameters
    setNode(nodeId)
    initParameters
    preparedStatement.executeQuery
  }
}

class ReadingIterator(resultSet: ResultSet) extends Iterator[Reading] {
  def hasNext = resultSet.next
  def next = {
    Reading(resultSet.getDouble("timestamp"),
      resultSet.getInt("node"),
      resultSet.getDouble("reading"))
  }
}

class ReadingQuery(connection: Connection, readingName: String) extends {
  val EVENTS_PER_SECOND = 1
  //NOTE: we mod timestamp by 1 to get data only once per second
  val statement = s"SELECT * from $readingName where node = ? and timestamp % $EVENTS_PER_SECOND = 0"
} with JdbcQuery(connection, statement) {
  def loadReadings(nodeId: Int) = {
    val resultSet = getForNode(nodeId)
    new ReadingIterator(resultSet)
      .toStream
  }
}

class PosXQuery(connection: Connection) extends ReadingQuery(connection, "posx")
class PosYQuery(connection: Connection) extends ReadingQuery(connection, "posy")
class SpeedQuery(connection: Connection) extends ReadingQuery(connection, "speed")