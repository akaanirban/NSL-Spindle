package edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store

import scala.concurrent.duration._
import edu.rpi.cs.nsl.spindle.vehicle.Types._
import java.sql.ResultSet
import java.sql.Connection

case class ClusterMembership(timestamp: FiniteDuration, clusterHead: NodeId) extends TSEntry[NodeId](timestamp) {
  def getReading: NodeId = clusterHead
}

class ClusterMemberIterator(resultSet: ResultSet)
    extends QueryIterator[ClusterMembership](resultSet) {
  def next: ClusterMembership = {
    ClusterMembership(timestamp = resultSet.getDouble("timestamp") seconds,
      clusterHead = resultSet.getInt("clusterhead"))
  }
}

class ClusterMembershipQuery(connection: Connection, clusterTable: String) extends {
  private val statement = s"""SELECT
       timestamp, 
       clusterhead 
     FROM $clusterTable
     WHERE node = ?
     ORDER BY timestamp"""
} with JdbcQuery(connection, statement) {
  def loadClusters(nodeId: Int): Stream[ClusterMembership] = {
    setNode(nodeId)
    val nodeStream = new ClusterMemberIterator(executeQuery).toStream
    assert(nodeStream.headOption.isDefined, s"No cluster head information found for $nodeId")
    nodeStream
  }
}
