package edu.rpi.cs.nsl.spindle.vehicle.simulation

import edu.rpi.cs.nsl.spindle.vehicle.Types._
import akka.actor.ActorLogging
import akka.actor.Actor
import akka.event.Logging
import akka.actor.Props
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.postgres.PgClient
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.PositionCache
import edu.rpi.cs.nsl.spindle.vehicle.simulation.sensors.SensorFactory
import edu.rpi.cs.nsl.spindle.vehicle.kafka.ClientFactory
import edu.rpi.cs.nsl.spindle.vehicle.simulation.properties.PropertyFactory
import scala.concurrent.duration._
import akka.dispatch.RequiresMessageQueue
import akka.dispatch.BoundedMessageQueueSemantics
import akka.actor.Stash

/**
 * Manages vehicles
 */
object World {
  case class InitSimulation()
  case class Starting()
  case class Ready()
  def props(propertyFactory: PropertyFactory, clientFactory: ClientFactory) = {
    Props(new World(propertyFactory: PropertyFactory, clientFactory))
  }
}

class World(propertyFactory: PropertyFactory, clientFactory: ClientFactory)
    extends Actor with ActorLogging with RequiresMessageQueue[BoundedMessageQueueSemantics] {
  import World._
  import Vehicle.{ StartMessage, ReadyMessage }
  private val logger = Logging(context.system, this)
  private[simulation] lazy val vehicles = {
    logger.debug("Creating vehicle actors")
    val pgClient = new PgClient()
    val vehicles = pgClient.getNodes.map { nodeId: NodeId =>
      val positions = new PositionCache(nodeId, pgClient)
      val timestamps = positions.getTimestamps.toList
      val mockSensors = SensorFactory.mkSensors(nodeId)
      val properties = propertyFactory.getProperties(nodeId)
      val actor = context.actorOf(Vehicle.props(nodeId,
        clientFactory,
        timestamps,
        positions,
        mockSensors,
        properties))
      context.watch(actor)
      (nodeId, actor)
    }
      .force
      .toList
    logger.info("Created all vehicles")
    vehicles
  }

  def receive = initializing

  private lazy val startTime = {
    logger.info("Creating start time")
    Configuration.simStartOffsetMs + System.currentTimeMillis()
  }

  def initializing: Receive = {
    case Ping => {
      logger.info("Got ping")
      sender ! Ping
    }
    case InitSimulation => {
      logger.info("World recieved init message")
      // Initializes vehicles
      val numVehicles = vehicles.size
      logger.info(s"Created $numVehicles vehicles")
      sender ! Starting
      context.become(starting(Set()))
      vehicles.foreach {
        case (_, actorRef) =>
          actorRef ! Vehicle.CheckReadyMessage
      }
      logger.debug("Sent checkReady to all vehicles")
    }
    case _ => throw new RuntimeException(s"Received unexpected message")
  }

  def starting(readyVehicles: Set[NodeId]): Receive = {
    case ReadyMessage(nodeId) => {
      logger.debug(s"Got ready message from $nodeId")
      val newReadySet = readyVehicles ++ Set(nodeId)
      if (newReadySet.size == vehicles.length) {
        logger.info(s"All ${vehicles.length} vehicles are online")
        context.parent ! Ready
      } else {
        context.become(starting(newReadySet))
      }
    }
    case badMsg: Any => throw new RuntimeException(s"Received unexpected message $badMsg")
  }
}
