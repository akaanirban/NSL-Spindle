package edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store

import java.sql.ResultSet
import java.sql.Connection
import edu.rpi.cs.nsl.spindle.vehicle.Types._
import scala.concurrent.duration._
import edu.rpi.cs.nsl.spindle.vehicle.simulation.Configuration.Vehicles.eventsPerSecondMod

/**
 * Get nodes that have been online or are online
 */
class ConcurrentNodesQuery(connection: Connection) extends {
  private val statement = s"""SELECT 
      distinct(x.node)
    FROM posx x, posy y, speed s
    WHERE (x.timestamp = y.timestamp and y.timestamp = s.timestamp)
      and x.timestamp % $eventsPerSecondMod = 0
      and (x.node = y.node and y.node = s.node)
      and x.node != ?
      and x.timestamp <= (SELECT 
                    min(x.timestamp)
                  FROM posx x, posy y, speed s
                  WHERE (x.timestamp = y.timestamp and y.timestamp = s.timestamp)
                    and x.timestamp % $eventsPerSecondMod = 0
                    and (x.node = y.node and y.node = s.node) 
                    and x.node = ?)"""
} with JdbcQuery(connection, statement) {
  def loadConcurrentNodes(nodeId: NodeId): Stream[NodeId] = {
    setNode(nodeId)
    setNode(nodeId, argNum = 2)
    new NodeIdIterator(executeQuery).toStream
  }
}