package edu.rpi.cs.nsl.spindle.vehicle.simulation

import akka.actor.Props
import akka.actor.ActorLogging
import akka.actor.Actor
import edu.rpi.cs.nsl.spindle.vehicle.Types._

object VehicleConnection {
  def props(inNode: NodeId, outNode: NodeId) = Props(new VehicleConnection(inNode, outNode))
}
/**
 * Acts as a relay between vehicles and cluster head, with ability to drop and re-send
 */
class VehicleConnection(inNode: NodeId, outNode: NodeId) extends Actor with ActorLogging { //TODO
  //TODO: have simple consumer and simple producer
  //TODO: use consistent topic namer to map node id to topic
  def receive = {
    case _ => throw new RuntimeException("Not implemented")
  }
}