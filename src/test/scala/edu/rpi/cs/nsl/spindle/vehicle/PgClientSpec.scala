package edu.rpi.cs.nsl.spindle.vehicle

import org.scalatest.FlatSpec

import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.postgres.PgConfig
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.postgres.PgDefaults
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.postgres.PgClient

class PgClientSpec extends FlatSpec {
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
    val nodeId = 0
    assert(client.getXPositions(nodeId).size > 0)
    assert(client.getYPositions(nodeId).size > 0)
    assert(client.getSpeeds(nodeId).size > 0)
  }
}