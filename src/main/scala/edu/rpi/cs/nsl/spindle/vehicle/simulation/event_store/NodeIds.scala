package edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store

import java.sql.ResultSet
import java.sql.Connection

class NodeIdIterator(resultSet: ResultSet) extends QueryIterator[Int](resultSet) {
  def next = resultSet.getInt("node")
}

class MetadataQuery(connection: Connection) extends {
  private val statement = "SELECT DISTINCT(node) FROM posx"
} with JdbcQuery(connection, statement) {
  def loadNodeIds = {
    new NodeIdIterator(executeQuery).toStream
  }
}