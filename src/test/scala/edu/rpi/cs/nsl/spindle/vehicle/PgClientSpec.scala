package edu.rpi.cs.nsl.spindle.vehicle

import org.scalatest.FlatSpec

import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.postgres.PgClient
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.postgres.PgConfig
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.postgres.PgDefaults
import org.slf4j.LoggerFactory
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.TSEntryCache
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.Position
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.PgCacheLoader
import edu.rpi.cs.nsl.spindle.tags.UnderConstructionTest

class PgClientSpec extends FlatSpec {
  private val logger = LoggerFactory.getLogger(this.getClass)
  it should "load test config" in {
    assert(PgDefaults.config.isInstanceOf[PgConfig])
  }

  trait ConnectedClient {
    val client = new PgCacheLoader()
  }

  it should "connect to postgres" in new ConnectedClient {
    client.close
  }

  it should "default to read-only" in new ConnectedClient {
    assert(client.isReadOnly)
    client.close
  }

  it should "load readings through PgClient" in new ConnectedClient {
    (0 to 5) map { nodeId =>
      logger.debug(s"PgClient loading for $nodeId")
      val (timestamps, caches) = client.mkCaches(nodeId)
      assert(caches.size > 0)
      assert(timestamps.size > 0)
    }
    client.close
  }

  ignore should "cache position information" in new ConnectedClient {
    val testNode = client.getNodes.head
    client.mkCaches(testNode)
    fail("Not implemented")
    /*(0 to 5)
      .map { nodeId =>
        logger.debug(s"Position cache generating for $nodeId")
        new TSEntryCache[Position](client.getReadings(nodeId), _.toPosition)
      }
      .foreach { cache =>
        assert(cache.getTimestamps.size > 0)
      }
    client.close*/
  }

  it should "load a stream of node ids" in new ConnectedClient {
    try {
      val nodeStream = client.getNodes
      assert(nodeStream.size > 0)
      // Check that stream has been materialized
      assert(nodeStream.hasDefiniteSize)
    } catch {
      case e: ArrayIndexOutOfBoundsException => {
        e.printStackTrace()
        throw e
      }
    }
    client.close
  }

  it should "load the simulation time range" taggedAs (UnderConstructionTest) in new ConnectedClient {
    val timeRange = client.getTimeRange
    assert(timeRange.minTime <= timeRange.maxTime)
    client.close
  }
}