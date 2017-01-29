package edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store

import edu.rpi.cs.nsl.spindle.vehicle.Types._
import java.sql.Connection
import java.sql.ResultSet
import org.slf4j.LoggerFactory

class JdbcQuery(connection: Connection, statement: String) {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val preparedStatement = {
    logger.trace(s"Preparing statement $statement")
    connection.prepareStatement(statement)
  }
  protected def setNode(node: NodeId, argNum: Int = 1) {
    logger.trace(s"Setting node $node (index $argNum) in $statement")
    preparedStatement.setInt(argNum, node)
  }
  protected def executeQuery: ResultSet = {
    logger.trace(s"Executing query $statement with $preparedStatement")
    preparedStatement.executeQuery
  }
}