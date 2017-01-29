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
import akka.actor.ActorRef
import edu.rpi.cs.nsl.spindle.vehicle.kafka.ClientFactoryConfig

object VehicleConnection {
  case class SetMappers(runningMapperIds: Set[String])
  case class SetClusterHead(clusterHead: NodeId)
  case class Ack()
  def props(inNode: NodeId, clientFactoryConfig: ClientFactoryConfig): Props = Props(new VehicleConnection(inNode, clientFactoryConfig))
}
/**
 * Acts as a relay between vehicles and cluster head, with ability to drop and re-send
 */
class VehicleConnection(inNode: NodeId, clientFactoryConfig: ClientFactoryConfig) extends Actor with ActorLogging { //TODO
  import VehicleConnection._
  import TopicLookupService.{ getClusterInput, getMapperOutput }
  private val logger = Logging(context.system, this)
  private val clientFactory = new ClientFactory(clientFactoryConfig)
  /**
   * TODO: have simple consumer and simple producer replace mapper in order to drop messages,
   * if some kind of filter operation cannot be done (see: Low level API for kafka streams as well)
   */

  def receive: PartialFunction[Any, Unit] = running()

  private def mkRelay(inMappers: Set[String], outNode: NodeId) = {
    val inTopics = inMappers.map(TopicLookupService.getMapperOutput(inNode, _))
    val outTopic = TopicLookupService.getClusterInput(outNode)
    val relayId = java.util.UUID.randomUUID.toString //TODO: check if we can re-use relays (prolly not)    
    val relay: StreamRelay = clientFactory.mkRelay(inTopics, outTopic, relayId)
    logger.debug(s"Node $inNode dispatching relay $relay and is sendign to $outTopic")
    relay.run
    logger.debug(s"Node $inNode relaying to $outNode via topics $inTopics -> $outTopic with $relay")
    relay
  }

  private def replaceRelay(inMappers: Set[String], outNode: NodeId, relayOpt: Option[StreamRelay], caller: ActorRef) {
    logger.debug(s"Node $inNode replacing relay and sending to $outNode")
    relayOpt match {
      case Some(relay) => {
        logger.debug(s"Node $inNode is stopping existing relay")
        context.dispatcher.execute(new Runnable() {
          def run {
            logger.info(s"Stopping stream in $inNode asynchronously")
            relay.stopStream //TODO: debug slowness
          }
        })
        logger.debug(s"Node $inNode has stopped existing relay")
      }
      case _ => {}
    }
    //caller ! Ack() //TODO
    logger.debug(s"Node $inNode has replaced relay")
    val newRelayOpt = Some(mkRelay(inMappers, outNode))
    logger.debug(s"Node $inNode has replaced relay")

    context.become(running(inMappers, outNode, newRelayOpt))
  }

  /**
   * Receive updates
   *
   * @note - multiple reducers can share a single topic without issues
   */
  def running(inMappers: Set[String] = Set(), outNode: NodeId = inNode, relayOpt: Option[StreamRelay] = None): Receive = {
    case Ping() => sender ! Ping()
    case SetMappers(newIds) => {
      sender ! Ack()
      (inMappers == newIds) match {
        case false => replaceRelay(newIds, outNode, relayOpt, sender)
        case _     => {}
      }
    }
    case SetClusterHead(newId) => {
      sender ! Ack()
      (newId == outNode) match {
        case false => replaceRelay(inMappers, newId, relayOpt, sender)
        case _     => {}
      }
    }
  }

}