package edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store

import edu.rpi.cs.nsl.spindle.vehicle.Types._
import java.sql.Connection
import java.sql.ResultSet

class JdbcQuery(connection: Connection, statement: String) {
  private val preparedStatement = connection.prepareStatement(statement)
  protected def setNode(node: NodeId, argNum: Int = 1) {
    preparedStatement.setInt(argNum, node)
  }
  protected def executeQuery: ResultSet = preparedStatement.executeQuery
}