package edu.rpi.cs.nsl.spindle.vehicle.simulation

import edu.rpi.cs.nsl.spindle.vehicle.Types._
import akka.actor.ActorLogging
import akka.actor.Actor
import akka.event.Logging
import akka.actor.Props
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.postgres.PgClient
import edu.rpi.cs.nsl.spindle.vehicle.simulation.sensors.SensorFactory
import edu.rpi.cs.nsl.spindle.vehicle.kafka.ClientFactory
import edu.rpi.cs.nsl.spindle.vehicle.simulation.properties.PropertyFactory
import scala.concurrent.duration._
import akka.dispatch.RequiresMessageQueue
import akka.dispatch.BoundedMessageQueueSemantics
import akka.actor.Stash
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.Await
import akka.actor.ActorRef
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import java.util.concurrent.TimeoutException
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.CacheFactory

/**
 * Manages vehicles
 */
object World {
  case class InitSimulation()
  case class Starting()
  case class Ready(numVehicles: Int)
  def props(propertyFactory: PropertyFactory, clientFactory: ClientFactory) = {
    Props(new World(propertyFactory: PropertyFactory, clientFactory))
  }
  val VEHICLE_WAIT_TIME = 10 seconds
}

class World(propertyFactory: PropertyFactory, clientFactory: ClientFactory, maxVehicles: Option[Int] = None)
    extends Actor with ActorLogging with RequiresMessageQueue[BoundedMessageQueueSemantics] {
  import World._
  import Vehicle.{ StartMessage, ReadyMessage }
  private val logger = Logging(context.system, this)
  private[simulation] lazy val vehicles = {
    logger.debug("Creating vehicle actors")
    val pgClient = new PgClient()
    val nodeList = maxVehicles match {
      case None      => pgClient.getNodes
      case Some(max) => pgClient.getNodes.take(max)
    }
    val vehicles = nodeList.map { nodeId: NodeId =>
      val cacheFactory = new CacheFactory(pgClient)
      //val positions = new PositionCache(nodeId, pgClient)
      val mockSensors = SensorFactory.mkSensors(nodeId)
      val properties = propertyFactory.getProperties(nodeId)
      val actor = context.actorOf(Vehicle.props(nodeId,
        clientFactory,
        cacheFactory,
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

  private implicit val vehicleTimeout = Timeout(VEHICLE_WAIT_TIME)
  private implicit val ec = context.dispatcher

  private def tryCheck(actorRef: ActorRef): Future[_] = {
    logger.debug(s"Checking $actorRef")
    val checkFuture = actorRef ? Vehicle.CheckReadyMessage
    checkFuture
      .recover {
        case err => {
          logger.warning("Failed to send message to vehicle. Retrying.")
          tryCheck(actorRef)
        }
      }
  }

  private def becomeStarted(supervisor: ActorRef) {
    logger.info(s"All ${vehicles.size} vehicles ready")
    supervisor ! Ready(vehicles.size)
    context.become(started)
  }

  private def checkReady(supervisor: ActorRef) {
    val replyFutures = Future.sequence {
      vehicles
        .map(_._2)
        .map(tryCheck)
    }
    logger.debug(s"Checking on all vehicles")
    replyFutures.onSuccess {
      case _ => {
        becomeStarted(supervisor)
      }
    }
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
      checkReady(sender)
    }
    case _ => throw new RuntimeException(s"Received unexpected message (start mode)")
  }
  def started: Receive = {
    case _ => throw new RuntimeException(s"Received unexpected message (started mode)")
  }
}
