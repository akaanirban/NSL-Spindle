package edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store

import java.sql.ResultSet
import java.sql.Connection
import edu.rpi.cs.nsl.spindle.vehicle.Types._

class NodeIdIterator(resultSet: ResultSet) extends QueryIterator[NodeId](resultSet) {
  def next: Int = resultSet.getInt("node")
}

class MetadataQuery(connection: Connection) extends {
  private val statement = "SELECT DISTINCT(node) FROM posx"
} with JdbcQuery(connection, statement) {
  def loadNodeIds: Stream[NodeId] = {
    new NodeIdIterator(executeQuery).toStream //TODO: ensure returning string doesn't cause memory leak
  }
}