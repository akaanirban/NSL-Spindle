package edu.rpi.cs.nsl.spindle.vehicle

import org.scalatest.FlatSpec

import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.postgres.PgClient
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.postgres.PgConfig
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.postgres.PgDefaults
import org.slf4j.LoggerFactory
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.PositionCache

class PgClientSpec extends FlatSpec {
  private val logger = LoggerFactory.getLogger(this.getClass)
  it should "load test config" in {
    assert(PgDefaults.config.isInstanceOf[PgConfig])
  }

  trait ConnectedClient {
    val client = new PgClient()
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
      val readings = client.getReadings(nodeId)
      assert(readings.size > 0)
    }
    client.close
  }

  it should "cache position information" in new ConnectedClient {
    (0 to 5)
      .map { nodeId =>
        logger.debug(s"Position cache generating for $nodeId")
        new PositionCache(nodeId, client)
      }
      .foreach { cache =>
        assert(cache.getTimestamps.size > 0)
      }
    client.close
  }

  it should "load a stream of node ids" in new ConnectedClient {
    val nodeStream = client.getNodes
    assert(nodeStream.size > 0)
    assert(nodeStream.hasDefiniteSize)
    client.close
  }
}