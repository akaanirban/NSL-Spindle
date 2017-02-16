package edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store

import java.sql.ResultSet
import java.sql.Connection

import edu.rpi.cs.nsl.spindle.vehicle.Types._
import edu.rpi.cs.nsl.spindle.vehicle.simulation.Configuration
import org.slf4j.LoggerFactory

class NodeIdIterator(resultSet: ResultSet) extends QueryIterator[NodeId](resultSet) {
  private val logger = LoggerFactory.getLogger(this.getClass)
  def next: Int = {
    logger.trace(s"Loading next node id from $resultSet")
    resultSet.getInt("node")
  }
}

class NodeIdsQuery(connection: Connection) extends {
  //note: timestamp selection is to limit memory consumption
  private val statement = s"""SELECT
      DISTINCT(x.node) as node
    FROM posx x, posy y, speed s, ${Configuration.Vehicles.activeNodesTabele} a
    WHERE (x.node = y.node and y.node = s.node and s.node = a.node)
      and (x.timestamp = y.timestamp and y.timestamp = s.timestamp)"""
} with JdbcQuery(connection, statement) {
  private val logger = LoggerFactory.getLogger(this.getClass.toString)
  def loadNodeIds: Iterable[NodeId] = {
    logger.debug("Loading node ids")
    new NodeIdIterator(executeQuery).toStream //TODO: ensure returning stream doesn't cause memory leak
  }
}