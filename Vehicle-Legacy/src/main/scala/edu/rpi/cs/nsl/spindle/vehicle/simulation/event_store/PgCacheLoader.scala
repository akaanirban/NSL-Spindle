package edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store

import CacheTypes._
import edu.rpi.cs.nsl.spindle.vehicle.simulation.Configuration
import edu.rpi.cs.nsl.spindle.vehicle.Types._
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.postgres.PgClient
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.postgres.PgConfig
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.postgres.PgDefaults
import org.slf4j.LoggerFactory

class PgCacheLoader(config: PgConfig = PgDefaults.config) extends PgClient(config) with EventStore {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private lazy val positionQuery = new PositionQuery(connection)
  private lazy val clusterHeadQuery = new ClusterMembershipQuery(connection, Configuration.Vehicles.clusterMemberTable)
  private lazy val metadataQuery = new NodeIdsQuery(connection)
  private lazy val concurrentQuery = new ConcurrentNodesQuery(connection)
  private lazy val timeRangeQuery = new TimeRangeQuery(connection)

  lazy val timeRange: TimeRange = timeRangeQuery.loadTimeMinMax
  def getMinSimTime: Timestamp = timeRange.minTime.toMillis
  def getNodes: Iterable[NodeId] = metadataQuery.loadNodeIds
  def mkCaches(nodeId: NodeId): (Seq[Timestamp], CacheMap) = {
    logger.debug(s"Creating caches for $nodeId")
    val positionCache = new TSEntryCache[Position](positionQuery.loadReadings(nodeId))
    val clusterHeadCache = new TSEntryCache[NodeId](clusterHeadQuery.loadClusters(nodeId))
    val cacheMap = Map(PositionCache -> positionCache, ClusterCache -> clusterHeadCache)
    val timestamps = positionCache.getTimestamps.toSeq.sorted
    assert(timestamps.head <= timestamps.last, s"${timestamps.head} not less than ${timestamps.last}")
    logger.debug(s"Created cache for $nodeId")
    (timestamps, cacheMap)
  }
  def getConcurrentNodes(nodeId: NodeId): Iterable[NodeId] = concurrentQuery
    .loadConcurrentNodes(nodeId)
}