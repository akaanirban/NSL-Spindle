package edu.rpi.cs.nsl.spindle.vehicle.simulation

import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.PositionCache
import org.slf4j.LoggerFactory

case class SimulatorData(nodeId: Int, positions: PositionCache, sensors: List[Null]) //TODO: sensors

/**
 * Simulates an individual vehicle
 */
class Vehicle(data: SimulatorData) extends Runnable { //TODO: starting time, kafka references
  import data.nodeId
  private val logger = LoggerFactory.getLogger(s"${this.getClass.getName}-$nodeId")
  def run {
    logger.info(s"$nodeId starting")
    //TODO
    throw new RuntimeException("Not implemented")
  }
}

//TODO: static vehicle (one cluster per vehicle)
//TODO: database vehicle (interface with postgres)