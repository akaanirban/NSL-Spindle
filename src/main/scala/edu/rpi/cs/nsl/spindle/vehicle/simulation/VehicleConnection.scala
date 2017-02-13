package edu.rpi.cs.nsl.spindle.vehicle.simulation

import java.util.concurrent.Executors

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.event.Logging
import edu.rpi.cs.nsl.spindle.vehicle.Types.NodeId
import edu.rpi.cs.nsl.spindle.vehicle.kafka.ClientFactory
import edu.rpi.cs.nsl.spindle.vehicle.kafka.ClientFactoryConfig
import edu.rpi.cs.nsl.spindle.vehicle.kafka.streams.StreamRelay
import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.TopicLookupService

object VehicleConnection {
  case class SetMappers(runningMapperIds: Set[String])
  case class SetClusterHead(clusterHead: NodeId)
  case class Ack()
  case class CloseConnection()
  def props(inNode: NodeId, clientFactoryConfig: ClientFactoryConfig): Props = Props(new VehicleConnection(inNode, clientFactoryConfig))
}
/**
 * Acts as a relay between vehicles and cluster head, with ability to drop and re-send
 */
class VehicleConnection(inNode: NodeId, clientFactoryConfig: ClientFactoryConfig) extends Actor with ActorLogging {
  import VehicleConnection._
  import TopicLookupService.{ getClusterInput, getMapperOutput }
  private val logger = Logging(context.system, this)
  private val clientFactory = new ClientFactory(clientFactoryConfig)
  import context.dispatcher
  private val pool = Executors.newSingleThreadExecutor()
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
        pool.execute(new Runnable() {
          def run {
            logger.info(s"Stopping stream in $inNode asynchronously")
            relay.stopStream //TODO: debug slowness
            logger.info(s"Stopped stream in $inNode")
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
      if(inMappers != newIds)  {
        replaceRelay(newIds, outNode, relayOpt, sender)
      }
    }
    case SetClusterHead(newId) => {
      sender ! Ack()
      if(newId != outNode) {
        replaceRelay(inMappers, newId, relayOpt, sender)
      }
    }
    case CloseConnection() => {
      val replyActor: ActorRef = sender()
      logger.debug(s"Relay to $outNode shutting down")
      relayOpt match {
        case Some(streamRelay) => {
          streamRelay.stopStream.map{_ =>
            logger.info(s"Stream relay shut down. Alerting vehicle.")
            replyActor ! Ack()
          }
        }
        case None => {replyActor ! Ack()}
      }
    }
  }
}