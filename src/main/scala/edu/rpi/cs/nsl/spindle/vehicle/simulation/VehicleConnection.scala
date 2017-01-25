package edu.rpi.cs.nsl.spindle.vehicle.simulation

import akka.actor.Props
import akka.actor.ActorLogging
import akka.actor.Actor
import edu.rpi.cs.nsl.spindle.vehicle.Types._
import akka.event.Logging
import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.TopicLookupService
import edu.rpi.cs.nsl.spindle.vehicle.kafka.ClientFactory
import edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations.MapperFunc
import edu.rpi.cs.nsl.spindle.vehicle.kafka.streams.StreamRelay

object VehicleConnection {
  case class SetMappers(runningMapperIds: Set[String])
  case class SetClusterHead(clusterHead: NodeId)
  def props(inNode: NodeId, clientFactory: ClientFactory): Props = Props(new VehicleConnection(inNode, clientFactory))
}
/**
 * Acts as a relay between vehicles and cluster head, with ability to drop and re-send
 */
class VehicleConnection(inNode: NodeId, clientFactory: ClientFactory) extends Actor with ActorLogging { //TODO
  import VehicleConnection._
  import TopicLookupService.{ getClusterInput, getMapperOutput }
  private val logger = Logging(context.system, this)
  /**
   * TODO: have simple consumer and simple producer replace mapper in order to drop messages,
   * if some kind of filter operation cannot be done (see: Low level API for kafka streams as well)
   */

  def receive: PartialFunction[Any, Unit] = running()

  private def mkRelay(inMappers: Set[String], outNode: NodeId) = {
    val inTopics = inMappers.map(TopicLookupService.getMapperOutput(inNode, _))
    val outTopics = Set(TopicLookupService.getClusterInput(outNode))
    throw new RuntimeException("Not implemented")
  }

  private def replaceRelay(inMappers: Set[String], outNode: NodeId, relayOpt: Option[StreamRelay]) {
    relayOpt match {
      case Some(relay) => relay.stop
      case _           => {}
    }
    val newRelayOpt = Some(mkRelay(inMappers, outNode))
    context.become(running(inMappers, outNode, newRelayOpt))
  }

  /**
   * Receive updates
   *
   * @note - multiple reducers can share a single topic without issues
   */
  def running(inMappers: Set[String] = Set(), outNode: NodeId = inNode, relayOpt: Option[StreamRelay] = None): Receive = {
    case Ping() => sender ! Ping()
    case SetMappers(newIds) => (inMappers == newIds) match {
      case false => replaceRelay(newIds, outNode, relayOpt)
      case _     => {}
    }
    case SetClusterHead(newId) => (newId == outNode) match {
      case false => replaceRelay(inMappers, newId, relayOpt)
      case _     => {}
    }
  }

}