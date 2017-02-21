package edu.rpi.cs.nsl.spindle.vehicle.simulation

import java.util.concurrent.TimeUnit

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import akka.actor._
import akka.actor.Props
import akka.event.Logging
import akka.pattern.ask
import edu.rpi.cs.nsl.spindle.datatypes.{Vehicle => VehicleMessage}
import edu.rpi.cs.nsl.spindle.datatypes.VehicleTypes

import scala.concurrent.duration._
import edu.rpi.cs.nsl.spindle.vehicle.Types._
import edu.rpi.cs.nsl.spindle.vehicle.kafka.{ClientFactory, ClientFactoryConfig}
import edu.rpi.cs.nsl.spindle.vehicle.kafka.streams.StreamExecutor
import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.TopicLookupService
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.CacheTypes
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.EventStore
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.Position
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.TSEntryCache
import edu.rpi.cs.nsl.spindle.vehicle.simulation.sensors.MockSensor
import edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations.ActiveTransformations
import edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations.KvReducerFunc
import edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations.MapperFunc
import edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations.TransformationFunc
import edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations.TransformationStore
import akka.util.Timeout
import edu.rpi.cs.nsl.spindle.vehicle.TypedValue

import scala.concurrent.blocking

case class Ping()

object Vehicle {
  case class ReadyMessage(nodeId: NodeId, timestamps: Set[Timestamp])
  case class StartingMessage(nodeId: NodeId, eventTime: Timestamp = System.currentTimeMillis)
  case class FullShutdown()
  case class ShutdownComplete(nodeId: NodeId)

  private[simulation] case class TimeMapping(wallTime: Timestamp, simTime: Timestamp)
  case class Tick(simTime: Timestamp, tockDest: ActorRef)
  case class Tock(nodeId: NodeId, completedSimTime: Timestamp)
  // Sent by world
  case class CheckReadyMessage()
  def props(nodeId: NodeId,
            clientFactoryConfig: ClientFactoryConfig,
            transformationStore: TransformationStore,
            eventStore: EventStore,
            mockSensors: Set[MockSensor[Any]],
            properties: Set[TypedValue[Any]],
            warmCaches: Boolean = true): Props = {
    Props(new Vehicle(nodeId: NodeId,
      new ClientFactory(clientFactoryConfig): ClientFactory,
      transformationStore: TransformationStore,
      eventStore: EventStore,
      mockSensors: Set[MockSensor[Any]],
      properties: Set[TypedValue[Any]],
      warmCaches))
  }
  // Sent when execution completed
  case class SimulationDone(nodeId: NodeId)
}

object SchedulerUtils { //TODO: remove
  implicit class EnhancedScheduler(scheduler: Scheduler) {
    def scheduleAfterTime(timestamp: Timestamp, f: => Unit)(implicit ec: ExecutionContext) {
      val delayMs = (timestamp - System.currentTimeMillis()).toLong
      assert(delayMs >= 0, s"Time $timestamp has already passed (current time ${System.currentTimeMillis})")
      scheduler.scheduleOnce(Duration.create(delayMs, TimeUnit.MILLISECONDS))(f)
    }
  }
}

/**
 * Simulates an individual vehicle
 *
 * @todo - convert "sensors" to TSEntryCache? (refactor to expect data sometimes to be generated)
 *
 */
class Vehicle(nodeId: NodeId,
              clientFactory: ClientFactory,
              transformationStore: TransformationStore,
              eventStore: EventStore,
              mockSensors: Set[MockSensor[Any]],
              properties: Set[TypedValue[Any]],
              // Disable cache warming for faster tests
              warmCaches: Boolean = true)
    extends Actor with ActorLogging { //TODO: kafka references
  private lazy val logger = Logging(context.system, this)
  private lazy val fullProperties: Iterable[TypedValue[Any]] = {
    properties.toSeq ++
      Seq(TypedValue[VehicleTypes.VehicleId](nodeId)).asInstanceOf[Seq[TypedValue[Any]]]
  }
  import context.dispatcher
  private lazy val (timestamps, caches): (Seq[Timestamp], CacheMap) = {
    val (timestamps, caches) = eventStore.mkCaches(nodeId)
    (timestamps.take(Configuration.Vehicles.maxIterations.getOrElse(timestamps.length)), caches)
  }

  /**
   * Actor responsible for relaying data to cluster head
   */
  private lazy val clusterHeadConnection: ActorRef = {
    logger.debug(s"Node $nodeId creating cluster head connection actor")
    val actorRef: ActorRef = context.actorOf(VehicleConnection.props(nodeId, clientFactory.getConfig), name = s"clusterHeadConnector$nodeId")
    context.watch(actorRef)
    logger.debug(s"Node $nodeId has created cluster head connection actor")
    //NOTE: insert monitoring here (once message-based tick is set up)
    actorRef
  }

  private lazy val middlewareConnection = {
    logger.debug(s"$nodeId creating middleware connection relay")
    val inTopic = TopicLookupService.getClusterOutput(nodeId)
    val outTopic = TopicLookupService.middlewareInput
    val relayId = s"$nodeId-middleware-connection"
    val relay = clientFactory.mkRelay(Set(inTopic), outTopic, relayId)
    logger.debug(s"$nodeId starting middleware relay")
    relay.run
    logger.debug(s"$nodeId has started middleware relay")
    relay
  }

  /**
   * Create a Vehicle status message
   */
  private[simulation] def generateMessage(currentSimTime: Timestamp): VehicleMessage = {
    import VehicleTypes._
    import CacheTypes._
    val readings: Seq[TypedValue[Any]] = {
      val positionSeq = caches(PositionCache)
        .asInstanceOf[TSEntryCache[Position]]
        .getValueOpt(currentSimTime)
        .map { position =>
          val mph = TypedValue[MPH](position.speed)
          val lat = TypedValue[Lat](position.x)
          val lon = TypedValue[Lon](position.y)
          Seq[TypedValue[_]](mph, lat, lon)
        }
        .getOrElse {
          logger.error(s"No position information available for time $currentSimTime in timestamps $timestamps for cache ${caches(PositionCache).cache}")
          throw new RuntimeException(s"No position information for $currentSimTime")
        }

      mockSensors.toSeq.map(_.getReading(currentSimTime)) ++ positionSeq
    }.asInstanceOf[Seq[TypedValue[Any]]]
    VehicleMessageFactory.mkVehicle(readings, fullProperties)
  }

  private trait TemporalDaemon {
    def executeInterval(currentSimTime: Timestamp): Future[Any]
    def safeShutdown: Future[Any]
    def fullShutdown: Future[Any] = safeShutdown
  }

  private object SensorDaemon extends TemporalDaemon {
    private lazy val outTopic = TopicLookupService.getVehicleStatus(nodeId)
    private val producer = clientFactory.mkProducer[NodeId, VehicleMessage](outTopic)
    def executeInterval(currentSimTime: Timestamp): Future[Any] = {
      logger.debug(s"Vehicle $nodeId executing sensor daemon for $currentSimTime")
      val message: VehicleMessage = generateMessage(currentSimTime)
      logger.debug(s"Vehicle $nodeId generated message $message")
      val future = producer.send(nodeId, message)
      future.map(_ => logger.debug(s"Vehicle $nodeId sent status message"))
    }
    def safeShutdown: Future[Any] = {
      logger.debug(s"$nodeId shutting down sensor producer")
      Future {
        blocking {
          producer.flush
          producer.close
          logger.debug(s"$nodeId has shut down sensor producer")
        }
      }
    }
  }

  implicit val connectionActorTimeout = Timeout(2 seconds) //TODO: this should be within 1 second

  private object ClusterMembershipDaemon extends TemporalDaemon {
    private lazy val clusterCacheRef = caches(CacheTypes.ClusterCache).asInstanceOf[TSEntryCache[NodeId]]
    def executeInterval(currentSimTime: Timestamp): Future[Any] = {
      val clusterHead = clusterCacheRef.getOrPriorOpt(currentSimTime).getOrElse{
        logger.warning(s"No cluster head found for node $nodeId at time $currentSimTime: ${clusterCacheRef.cache}")
        nodeId
      }
      val future = (clusterHeadConnection ? VehicleConnection.SetClusterHead(clusterHead))
      logger.info(s"Node $nodeId has updated cluster head $clusterHead at time $currentSimTime")
      future.map(_ => None)
    }
    def safeShutdown: Future[Any] = {
      val shutdownTimeout = Timeout(Configuration.Vehicles.shutdownTimeout)
      logger.debug(s"Vehicle $nodeId is shutting down cluster-head relay")
      val downFuture = (clusterHeadConnection.ask(VehicleConnection.CloseConnection())(shutdownTimeout))
      downFuture.map {_ =>
        logger.info(s"Vehicle $nodeId closed clusterhead relay. Shutting down connection manager actor.")
        context.unwatch(clusterHeadConnection)
        context.stop(clusterHeadConnection)
        logger.debug(s"Vehicle $nodeId shut down cluster head relay")
        Unit
      }
    }
  }

  private object MapReduceDaemon extends TemporalDaemon {
    private var prevMappers: Map[MapperFunc[_, _, _, _], StreamExecutor] = Map()
    private var prevReducers: Map[KvReducerFunc[_, _], StreamExecutor] = Map()
    private def getChanges[T <: TransformationFunc](existing: Set[T], latest: Set[T]) = {
      val toRemove = existing diff latest
      val toAdd = latest diff existing
      (toRemove, toAdd)
    }
    private def updateTransformers[T <: TransformationFunc](toRun: Iterable[T], prevMap: Map[T, StreamExecutor]): Future[Map[T, StreamExecutor]] = {
      val (toRemove, toAdd) = getChanges(prevMap.keySet, toRun.toSet)
      Future.sequence(toRemove.map(prevMap(_)).map(_.stopStream)).map{_=>
        val newTransformers = toAdd.map(func => (func -> func.getTransformExecutor(clientFactory))).toMap
        newTransformers.values.foreach(_.run)
        newTransformers.values.foreach(transformer => logger.debug(s"Launched executor $transformer"))
        (prevMap -- toRemove) ++ newTransformers
      }
    }
    private def updateMappers(mappers: Iterable[MapperFunc[_, _, _, _]]): Future[Unit] =  {
      updateTransformers(mappers, prevMappers).map{nextMap =>
        assert(nextMap.keySet equals mappers.toSet)
        prevMappers = nextMap
      }
    }
    private def updateReducers(reducers: Iterable[KvReducerFunc[_, _]]): Future[Unit] = {
      updateTransformers(reducers, prevReducers).map{nextReducers =>
        assert(nextReducers.keySet equals reducers.toSet)
        prevReducers = nextReducers
      }
    }
    def executeInterval(currentSimTime: Timestamp): Future[Any] = {
      logger.debug(s"$nodeId map/reduce daemon updating for $currentSimTime")
      val (lat, lon) = {
        val message = generateMessage(currentSimTime)
        (message.lat, message.lon)
      }
      val ActiveTransformations(mappers, reducers) = transformationStore
        .getActiveTransformations(currentSimTime, (lat, lon))
      logger.debug(s"$nodeId updating mappers")
      val mapperFuture = updateMappers(mappers)
      logger.debug(s"$nodeId updating reducers")
      val reducerFuture = updateReducers(reducers)
      logger.debug(s"$nodeId updating cluster head connection with map/reduce changes")
      val clusterHeadFuture = (clusterHeadConnection ? VehicleConnection.SetMappers(mappers.map(_.funcId).toSet))
      logger.info(s"$nodeId updated mappers and reducers: $prevMappers $prevReducers")
      Future.sequence(Seq(mapperFuture, reducerFuture, clusterHeadFuture)).map{_ =>
        logger.info(s"Vehicle $nodeId updated all Map/Reduce threads for epoch $currentSimTime")
        Unit
      }
    }
    private def shutdownReducers: Future[Any] = {
      logger.debug(s"Vehicle $nodeId shutting down ${prevReducers.size} reducers: $prevReducers")
      val reducerShutdownFutures = prevReducers.values.toSeq.map { reducer =>
        logger.debug(s"Vehicle $nodeId closing reducer $reducer")
        reducer.stopStream.map { _ =>
          logger.debug(s"Vehicle $nodeId closed reducer $reducer")
          None
        }
      }
      Future.sequence(reducerShutdownFutures).map(_ => logger.debug(s"Vehicle $nodeId stopped reducers"))
    }
    def safeShutdown: Future[Any] = {
      logger.debug(s"Vehicle $nodeId shutting down ${prevMappers.size} mappers: $prevMappers")
      val shutdownFuture = Future.sequence(prevMappers.values.map(_.stopStream))
      logger.debug(s"Vehicle $nodeId scheduled shut down mappers")
      Configuration.Vehicles.shutdownReducersWhenComplete match {
        case true => shutdownFuture.flatMap(_ => shutdownReducers)
        case false => shutdownFuture
      }
    }

    override def fullShutdown: Future[Any] = {
      Future.sequence(Seq(super.fullShutdown, shutdownReducers))
    }
  }

  private lazy val temporalDaemons = {
    logger.info(s"$nodeId initializing temporal daemons")
    Seq(MapReduceDaemon, ClusterMembershipDaemon, SensorDaemon)
  }

  private def executeInterval(currentSimTime: Timestamp): Future[Any] = {
    logger.info(s"$nodeId executing for $currentSimTime: $temporalDaemons")
    temporalDaemons.foldLeft(Future.successful(None).asInstanceOf[Future[Any]]){case (prevFuture, daemon) =>
      logger.debug(s"$nodeId prev daemon completed. executing $daemon")
      prevFuture.flatMap(_ => daemon.executeInterval(currentSimTime))
    }
  }

  private def tick(simTime: Timestamp, tockDest: ActorRef) {
    logger.info(s"Vehicle ticking")
    val execFuture = executeInterval(simTime)

    execFuture.map{_ =>
      logger.info(s"$nodeId completed $simTime")
      tockDest ! Vehicle.Tock(nodeId, simTime)
    }
  }

  override def preStart {
    logger.debug(s"$nodeId running pre-start")
    if (warmCaches) {
      // Force evaluation of lazy properties
      val props = fullProperties
      logger.debug(s"$nodeId props: $props")
      val cacheTypes = this.caches.keys
      logger.debug(s"$nodeId cache types $cacheTypes")
      val numDaemons = temporalDaemons.size
      logger.debug(s"$nodeId has $numDaemons temporal daemons")
      logger.debug(s"Created cluster head connection ${this.clusterHeadConnection.actorRef}")
      logger.debug(s"Created middleware connection ${this.middlewareConnection}")
    }
    logger.debug(s"$nodeId finished pre-start")
  }

  def receive: PartialFunction[Any, Unit] = {
    case Ping() => {
      logger.debug("Replying to ping")
      sender ! Ping()
    }
    case Vehicle.CheckReadyMessage() => {
      logger.info(s"$nodeId got checkReady")
      sender ! Vehicle.ReadyMessage(nodeId, this.timestamps.toSet)
    }
    case Terminated(failedChild) => {
      logger.error(s"Child actor crashed for Vehicle $nodeId: $failedChild")
      throw new RuntimeException(s"Vehilce $nodeId child actor crashed")
    }
    case Vehicle.FullShutdown() => {
      val replyActor: ActorRef = sender()
      logger.info(s"Vehicle $nodeId is fully shutting down")
      val shutdownFutures = this.temporalDaemons.map(_.fullShutdown).zipWithIndex.map{case(future, index) =>
        future.map{_ =>
          logger.debug(s"Vehicle $nodeId has completed full shutdown of temporal daemon $index")
          Unit
        }
      }
      Future.sequence(shutdownFutures)
        .map(_ => replyActor ! Vehicle.ShutdownComplete(nodeId))
        .map(_ => this.clientFactory.close(nodeId))
        .map(_ => context.become(shutdown))
    }
    case VehicleConnection.Ack() => {
      logger.debug(s"Vehicle $nodeId got ack")
    }
    case Vehicle.Tick(simTime, tockDest) => {
      tick(simTime, tockDest)
    }
    case msg: Any => throw new RuntimeException(s"Unknown message on $nodeId: $msg")
  }

  def shutdown : PartialFunction[Any, Unit] = {
    case msg: Any => logger.warning(s"Got message after shutdown $msg")
  }
}