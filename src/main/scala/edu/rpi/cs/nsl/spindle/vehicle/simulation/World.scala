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
  case class Tick()
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
  import context.dispatcher
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
      clientFactory.getConfig,
      transformationStoreFactory.getTransformationStore(nodeId),
      new PgCacheLoader,
      mockSensors,
      properties,
      warmCaches = warmVehicleCaches))
    context.watch(actor)
    logger.debug(s"Created vehicle $nodeId")
    (nodeId, actor)
  }

  def receive: PartialFunction[Any, Unit] = initializing()

  private def mkTimeQueue(timingMap: Map[NodeId, Set[Timestamp]]): Seq[(Timestamp, Set[ActorRef])] = {
    val vehicleMap: Map[NodeId, ActorRef] = vehicles.toMap
    timingMap.toSeq
      .flatMap{case(node, timestamps) =>
        timestamps.map(ts => (ts, node))
      }
      .foldLeft(Map[Timestamp, Set[NodeId]]()){case (tsMap, (ts, node)) =>
        val newVal: Set[NodeId] = tsMap.getOrElse(ts, Set()) ++ Set(node)
          tsMap + (ts -> newVal)
      }
      .toSeq
      .sortBy(_._1)
      .map{case (ts, nodes) =>
        (ts, nodes.map(vehicleMap(_)))
      }

  }

  private def becomeStarted(supervisor: Option[ActorRef], timingMap: Map[NodeId, Set[Timestamp]]) {
    logger.info(s"All ${vehicles.size} vehicles ready")
    val timingQueue = mkTimeQueue(timingMap)
    tickVehicles(timingQueue.head)
    context.become(started(timingQueue = timingQueue, supervisor = supervisor))
  }

  private def checkReady() {
    logger.debug(s"Initializing vehicles and sending checkReady to ${vehicles.size} vehicles")
    vehicles.foreach {
      case (nodeId, actorRef) =>
        logger.debug(s"Doing tryCheck to $nodeId")
        actorRef ! Vehicle.CheckReadyMessage()
        logger.debug(s"$nodeId is being checked")
    }
  }

  private def tickVehicles(qHead: (Timestamp, Set[ActorRef])): Unit = qHead match {
    case (timestamp, actors) => actors.map(_ ! Vehicle.Tick(timestamp, self))
  }

  def initializing(initializer: Option[ActorRef] = None, timingMap: Map[NodeId, Set[Timestamp]] = Map()): Receive = {
    case Ping() => {
      logger.info("Got ping")
      sender ! Ping
    }
    case InitSimulation() => {
      logger.info("World received init message")
      try {
        checkReady()
        context.become(initializing(Some(sender), timingMap))
      } catch {
        case e: Exception => context.system.terminate()
      }
    }
    case Vehicle.ReadyMessage(nodeId, timestamps) => {
      logger.debug(s"$nodeId is ready")
      val newTimings: Map[NodeId, Set[Timestamp]] = timingMap + (nodeId -> timestamps)
      if(newTimings.keys.size == vehicles.size) {
        logger.info("All vehicles ready")
        initializer.get ! Ready(vehicles.size)
      }
      context.become(initializing(initializer, newTimings))
    }
    case StartSimulation => {
      sender ! Starting()
      becomeStarted(None, timingMap)
    }
    case StartSimulation(supervisor) => {
      sender ! Starting()
      becomeStarted(supervisor, timingMap)
    }
    case m: Any => throw new RuntimeException(s"Received unexpected message (start mode): $m")
  }

  private def shutdownVehicles: Future[Unit] = {
    logger.info("Shutting down all vehicles")
    implicit val timeout = Timeout(10 minutes)//TODO: no magic please
    Future.sequence(vehicles.map(_._2 ? Vehicle.FullShutdown())
      .map(_.map(_ => logger.info("World detected vehicle shutdown"))))
      .map(_ => Unit)
  }

  private def stopVehicles(): Unit = {
    val actorRefs = vehicles.map(_._2)
    logger.debug("Unwatching vehicles")
    actorRefs.map(context.unwatch(_))
    logger.debug("Unwatched vehicles. Stopping vehicle actors.")
    actorRefs.map(context.stop(_))
    logger.info("All vehicle actors stopped")
  }

  private case class BecomeFinished()

  private def becomeFinished(): Unit = {
    stopVehicles()
    this.clientFactory.close(-1)
    context.become(finished)
  }

  private def shutdown(supervisor: Option[ActorRef]): Unit = {
    logger.info("Simulation Commpleted")
    shutdownVehicles.map{_=>
      logger.info("All vehicles shut down")
      supervisor match {
        case Some(superRef) => superRef ! World.Finished()
        case _ => {}
      }
      self ! BecomeFinished()
    }
    context.system.scheduler.scheduleOnce(2 minutes, self, BecomeFinished())
  }


  def started(timingQueue: Seq[(Timestamp, Set[ActorRef])], finishedVehicles: Set[NodeId] = Set(), supervisor: Option[ActorRef]): Receive = {
    case Vehicle.ReadyMessage => logger.warning(s"Got extra ready message")
    case Vehicle.Tock(nodeId: NodeId, completedSimTime: Timestamp) => {
      assert(completedSimTime == timingQueue.head._1, s"Vehicle completed unexpected sim time $completedSimTime, timing queue $timingQueue")
      val newFinishedVehicles = finishedVehicles ++ Set(nodeId)
      if(newFinishedVehicles.size == timingQueue.head._2.size) {
        logger.info(s"All vehicles completed iteration $completedSimTime")
        val newQueue = timingQueue.tail
        if (newQueue.isEmpty) {
          shutdown(supervisor)
        } else {
          context.system.scheduler.scheduleOnce(1 seconds, self, World.Tick())
          context.become(started(newQueue, Set(), supervisor))
        }
      } else {
        context.become(started(timingQueue, newFinishedVehicles, supervisor))
      }
    }
    case BecomeFinished() => {
      logger.warning("Becoming finished after timeout")
      becomeFinished()
    }
    case CheckDone() => sender ! NotFinished()
    case Terminated(childActor) => {
      logger.warning(s"Child actor of world crashed: $childActor")
    }
    case World.Tick() => {
      logger.info("World ticking")
      tickVehicles(timingQueue.head)
    }
    case m: Any => {
      logger.error(s"Got unexpected message: $m")
    }
  }
  def finished: Receive = {
    case CheckDone() => sender ! Finished()
    case BecomeFinished() => {}
    case _ => {
      logger.warning("World got unexpected message after finished")
    }
  }
}
