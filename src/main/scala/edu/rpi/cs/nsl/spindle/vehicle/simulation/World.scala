package edu.rpi.cs.nsl.spindle.vehicle.simulation

import edu.rpi.cs.nsl.spindle.vehicle.Types._
import akka.actor.{Actor, ActorLogging, ActorRef, Kill, Props, Stash, Terminated}
import akka.event.Logging
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.postgres.PgClient
import edu.rpi.cs.nsl.spindle.vehicle.simulation.sensors.SensorFactory
import edu.rpi.cs.nsl.spindle.vehicle.kafka.ClientFactory
import edu.rpi.cs.nsl.spindle.vehicle.simulation.properties.PropertyFactory

import scala.concurrent.duration._
import akka.dispatch.RequiresMessageQueue
import akka.dispatch.BoundedMessageQueueSemantics
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import java.util.concurrent.TimeoutException

import edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations.TransformationStore
import edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations.TransformationStoreFactory
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.PgCacheLoader

/**
 * Manages vehicles
 */
object World {
  case class InitSimulation()
  case class Ready(numVehicles: Int)
  case class StartSimulation(supervisor: Option[ActorRef])
  case class Starting()
  case class Finished()
  case class NotFinished()
  case class CheckDone()
  def props(propertyFactory: PropertyFactory,
            transformationStoreFactory: TransformationStoreFactory,
            clientFactory: ClientFactory,
            maxNodes: Option[Int] = None): Props = {
    Props(new World(propertyFactory: PropertyFactory,
      transformationStoreFactory: TransformationStoreFactory,
      clientFactory,
      maxVehicles = maxNodes))
  }
  def propsTest(propertyFactory: PropertyFactory,
                transformationStoreFactory: TransformationStoreFactory,
                clientFactory: ClientFactory, initOnly: Boolean = true): Props = {
    Props(new World(propertyFactory: PropertyFactory,
      transformationStoreFactory: TransformationStoreFactory, clientFactory, initOnly = initOnly))
  }
  val VEHICLE_WAIT_TIME = 3 seconds
}

class World(propertyFactory: PropertyFactory,
            transformationStoreFactory: TransformationStoreFactory,
            clientFactory: ClientFactory,
            initOnly: Boolean = false,
            maxVehicles: Option[Int] = None)
    extends Actor with ActorLogging with RequiresMessageQueue[BoundedMessageQueueSemantics] {
  import World._
  import Vehicle.{ StartMessage, ReadyMessage }
  private val logger = Logging(context.system, this)
  private lazy val pgClient = new PgCacheLoader()
  private lazy val nodeList = maxVehicles match {
    case None => {
      logger.warning("World is loading all nodes")
      pgClient.getNodes
    }
    case Some(max) => pgClient.getNodes.toList.sorted.take(max)
  }
  private val warmVehicleCaches = (initOnly == false)
  private[simulation] lazy val vehicles: Iterable[(NodeId, ActorRef)] = nodeList.map { nodeId: NodeId =>
    logger.debug(s"Creating vehicle $nodeId")
    //val positions = new PositionCache(nodeId, pgClient)
    val mockSensors = SensorFactory.mkSensors(nodeId)
    val properties = propertyFactory.getProperties(nodeId)
    val actor = context.actorOf(Vehicle.props(nodeId,
      clientFactory,
      transformationStoreFactory.getTransformationStore(nodeId),
      new PgCacheLoader,
      mockSensors,
      properties,
      warmCaches = warmVehicleCaches))
    context.watch(actor)
    logger.debug(s"Created vehicle $nodeId")
    (nodeId, actor)
  }

  def receive: PartialFunction[Any, Unit] = initializing

  private implicit val vehicleTimeout = Timeout(VEHICLE_WAIT_TIME)
  private implicit val ec = context.dispatcher

  private def trySend[T](actorRef: ActorRef, message: Any, numAttempts: Int = 1): Future[T] = {
    val MAX_RETRIES = 20
    val MAX_WAIT_BEFORE_RETRY_MS = 1000
    if (numAttempts > MAX_RETRIES) {
      throw new RuntimeException(s"Failed to send message to $actorRef")
    }
    logger.debug(s"Sending $message to $actorRef")
    val sendFuture = (actorRef ? message).map(_.asInstanceOf[T])
    sendFuture
      .recover {
        case err => {
          logger.warning(s"Failed to send message to vehicle $actorRef $numAttempts. Retrying.")
          Thread.sleep((MAX_WAIT_BEFORE_RETRY_MS * Math.random).toLong) // Random wait to avoid livelock
          Await.result(trySend[T](actorRef, message, numAttempts = (numAttempts + 1)), Duration.Inf) //TODO: double-check best practices for this
        }
      }
  }

  private def tryCheck(actorRef: ActorRef): Future[_] = {
    logger.debug(s"Sending check message to $actorRef")
    trySend[Vehicle.ReadyMessage](actorRef, Vehicle.CheckReadyMessage)
  }

  private def tryStart(actorRef: ActorRef, startMessage: Vehicle.StartMessage): Future[_] = {
    logger.debug(s"Starting $actorRef")
    trySend[Vehicle.StartingMessage](actorRef, startMessage)
  }

  private def handleFailure {
    logger.error("Simulation failed. Shutting down")
    vehicles.foreach(_._2 ! Kill)
  }

  private def startAll(startTime: Timestamp) {
    val startRepliesFuture = Future.sequence {
      vehicles.map(tup => tryStart(tup._2, Vehicle.StartMessage(startTime, Some(context.self))))
    }
    val replies = Await.result(startRepliesFuture, Configuration.simStartOffsetMs milliseconds)
    val tooLate = replies.forall(_.asInstanceOf[Vehicle.StartingMessage].eventTime < startTime) == false
    if (tooLate) {
      logger.error(s"Vehicle(s) started too late")
      handleFailure
    }
  }

  private def becomeStarted(supervisor: Option[ActorRef]) {
    logger.info(s"All ${vehicles.size} vehicles ready")
    if (initOnly == false) {
      val startTime = System.currentTimeMillis + Configuration.simStartOffsetMs
      logger.info(s"Starting at epoch time $startTime")
      startAll(startTime)
      context.become(started(startTime, vehicles.size, supervisor = supervisor))
    } else {
      logger.error(s"TEST MODE: not starting simulation")
    }
  }

  private def checkReady() {
    logger.debug("Initializing vehicles and sending checkReady")
    vehicles.foreach {
      case (nodeId, actorRef) =>
        logger.debug(s"Doing tryCheck to $nodeId")
        Await.result(tryCheck(actorRef), Duration.Inf)
        logger.debug(s"$nodeId is ready")
    }
  }

  def initializing: Receive = {
    case Ping => {
      logger.info("Got ping")
      sender ! Ping
    }
    case InitSimulation => {
      logger.info("World recieved init message")
      try {
        checkReady()
        sender ! Ready(vehicles.size)
      } catch {
        case e: Exception => context.system.terminate()
      }
    }
    case StartSimulation => {
      sender ! Starting()
      becomeStarted(None)
    }
    case StartSimulation(supervisor) => {
      sender ! Starting()
      becomeStarted(supervisor)
    }
    case m: Any => throw new RuntimeException(s"Received unexpected message (start mode): $m")
  }
  def started(startTime: Double, numVehicles: Int, finishedVehicles: Set[NodeId] = Set(), supervisor: Option[ActorRef]): Receive = {
    case Vehicle.ReadyMessage => logger.warning(s"Got extra ready message")
    case Vehicle.SimulationDone(nodeId) => {
      logger.debug(s"World got finished message from $nodeId")
      val newFinished: Set[NodeId] = finishedVehicles + nodeId
      if (newFinished.size == numVehicles) {
        logger.info(s"All vehicles completed. Alerting $supervisor")
        logger.info("Shutting down vehicles")
        implicit val shutdownTimeout = Timeout(Configuration.Vehicles.shutdownTimeout * 2)
        Await.result(Future.sequence(vehicles.map(_._2.ask(Vehicle.FullShutdown())(shutdownTimeout))), Duration.Inf)
        logger.info("All vehicles shut down. Terminating vehicle actors.")
        val vehicleRefs = vehicles.map(_._2)
        vehicleRefs.foreach(context.unwatch)
        vehicleRefs.foreach(context.stop)
        logger.info("Stopped all vehicle actors.")
        supervisor match {
          case Some(actorRef) => actorRef ! Finished
          case None           => {
            logger.warning("World has completed with no supervision")
          }
        }
        logger.info("World becoming finished")
        context.become(finished)
      } else {
        logger.debug(s"${newFinished.size} vehicles finished of $numVehicles")
        context.become(started(startTime, numVehicles, newFinished, supervisor))
      }
    }
    case CheckDone() => sender ! NotFinished()
    case Terminated(childActor) => {
      logger.warning(s"Child actor of world crashed: $childActor")
    }
    case m: Any => {
      logger.error(s"Got unexpected message: $m")
    }
  }
  def finished: Receive = {
    case CheckDone() => sender ! Finished()
  }
}
