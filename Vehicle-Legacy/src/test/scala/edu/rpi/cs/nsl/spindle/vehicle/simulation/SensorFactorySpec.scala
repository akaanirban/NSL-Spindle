package edu.rpi.cs.nsl.spindle.vehicle.simulation

import org.scalatest.FlatSpec
import edu.rpi.cs.nsl.spindle.vehicle.simulation.sensors.SensorFactory
import org.slf4j.LoggerFactory

class SensorFactorySpec extends FlatSpec {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val TEST_NODE_ID = 0 //NOTE: may need to update if node 0 not supported
  it should "create a set of mock sensors from the config file" in {
    logger.info(s"Making sensors for node $TEST_NODE_ID")
    val sensors = SensorFactory.mkSensors(TEST_NODE_ID)
    logger.debug(s"Got sensors: $sensors")
  }
}