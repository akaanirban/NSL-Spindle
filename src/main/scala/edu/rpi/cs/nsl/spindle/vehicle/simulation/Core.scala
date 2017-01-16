package edu.rpi.cs.nsl.spindle.vehicle.simulation

import akka.actor.ActorSystem

/**
 * Driver for vehicle simulator
 */
object Core {
  private val actorSystem = ActorSystem("SpindleSimulator")
  def main(args: Array[String]) {
    //TODO
    actorSystem.shutdown()
  }
}