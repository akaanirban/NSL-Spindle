package edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store

import java.sql.Connection
import java.sql.ResultSet

class JdbcQuery(connection: Connection, statement: String) {
  private val preparedStatement = connection.prepareStatement(statement)
  protected def setNode(node: Int) {
    preparedStatement.setInt(1, node)
  }
  protected def executeQuery: ResultSet = preparedStatement.executeQuery
}