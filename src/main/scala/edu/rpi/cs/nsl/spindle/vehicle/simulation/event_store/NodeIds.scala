package edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store

import java.sql.ResultSet
import java.sql.Connection
import edu.rpi.cs.nsl.spindle.vehicle.Types._
import org.slf4j.LoggerFactory

class NodeIdIterator(resultSet: ResultSet) extends QueryIterator[NodeId](resultSet) {
  private val logger = LoggerFactory.getLogger(this.getClass)
  def next: Int = {
    logger.trace(s"Loading next node id from $resultSet")
    resultSet.getInt("node")
  }
}

class MetadataQuery(connection: Connection) extends {
  //note: timestamp selection is to limit memory consumption
  private val statement = """SELECT 
      DISTINCT(x.node) as node
    FROM posx x, posy y, speed s 
    WHERE (x.node = y.node and y.node = s.node) 
      and (x.timestamp = y.timestamp and y.timestamp = s.timestamp)"""
} with JdbcQuery(connection, statement) {
  def loadNodeIds: Iterable[NodeId] = {
    new NodeIdIterator(executeQuery).toStream //TODO: ensure returning stream doesn't cause memory leak
  }
}