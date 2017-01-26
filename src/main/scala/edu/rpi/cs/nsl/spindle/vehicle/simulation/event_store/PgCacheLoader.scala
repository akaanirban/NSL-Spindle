package edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store

import CacheTypes._
import edu.rpi.cs.nsl.spindle.vehicle.simulation.Configuration
import edu.rpi.cs.nsl.spindle.vehicle.Types._
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.postgres.PgClient
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.postgres.PgConfig
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.postgres.PgDefaults

class PgCacheLoader(config: PgConfig = PgDefaults.config) extends PgClient(config) with EventStore {
  private lazy val positionQuery = new PositionQuery(connection)
  private lazy val clusterHeadQuery = new ClusterMembershipQuery(connection, Configuration.Vehicles.clusterMemberTable)
  private lazy val metadataQuery = new MetadataQuery(connection)
  def getNodes: Stream[Int] = metadataQuery.loadNodeIds
  def mkCaches(nodeId: NodeId): (Seq[Timestamp], CacheMap) = {
    val positionCache = new TSEntryCache[Position](positionQuery.loadReadings(nodeId))
    val clusterHeadCache = new TSEntryCache[NodeId](clusterHeadQuery.loadClusters(nodeId))
    val cacheMap = Map(PositionCache -> positionCache, ClusterCache -> clusterHeadCache)
    val timestamps = cacheMap.values.map(_.getTimestamps.toSet).reduce(_ ++ _).toSeq.sorted //TODO: ensure timestamps are sorted in right order
    (timestamps, cacheMap)
  }
}