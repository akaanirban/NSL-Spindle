package edu.rpi.cs.nsl.spindle.vehicle.simulation

import akka.actor.ActorSystem
import edu.rpi.cs.nsl.spindle.vehicle.Types.NodeId

/**
 * Manages vehicles
 */
class World {
  private val actorSystem = ActorSystem("VehicleSimWorld")
  def mkVehicle {
    //TODO
    throw new RuntimeException("Not implemented")
  }
}