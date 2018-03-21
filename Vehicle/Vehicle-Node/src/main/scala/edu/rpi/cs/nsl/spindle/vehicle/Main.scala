package edu.rpi.cs.nsl.spindle.vehicle

import java.io.File
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent._

import edu.rpi.cs.nsl.spindle.datatypes.VehicleTypes.MPH
import edu.rpi.cs.nsl.spindle.vehicle.Types.Timestamp
import edu.rpi.cs.nsl.spindle.vehicle.connections._
import edu.rpi.cs.nsl.spindle.vehicle.events.{GossipEvent, SensorProducer}
import edu.rpi.cs.nsl.spindle.vehicle.gossip.GossipRunner
import edu.rpi.cs.nsl.spindle.vehicle.kafka.KafkaQueryUtils._
import edu.rpi.cs.nsl.spindle.vehicle.kafka.executors.{ByteRelay, KVReducer, KafkaConnectionInfo, Mapper}
import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.TopicLookupService
import edu.rpi.cs.nsl.spindle.vehicle.queries.{Query, QueryLoader}
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

/**
  * Handle cluster connections on startup
  */
object StartupManager {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private def waitZkKafka(zkString: String, kafkaBrokers: List[Server]): (ZookeeperConnection, KafkaConnection) = {
    logger.debug(s"Connecting to zookeeper server: $zkString")
    val zkConnection = new ZookeeperConnection(zkString)
    zkConnection.openSync()//TODO: set reasonable timeout duration
    logger.info(s"Connected to zookeeper server: $zkString")
    val kafkaConnection = new KafkaConnection(brokers=kafkaBrokers, zkString=zkString)
    logger.info(s"Attempting to connect to kafka brokers $kafkaBrokers")
    kafkaConnection.openSync() //TODO: set timeout
    logger.info(s"Connected to kafka servers: $kafkaBrokers")
    (zkConnection, kafkaConnection)
  }
  def waitLocal: (ZookeeperConnection, KafkaConnection) = {
    waitZkKafka(Configuration.Local.zkString, List(Configuration.Local.kafkaBroker))
  }
  def waitCloud: (ZookeeperConnection, KafkaConnection) = {
    waitZkKafka(Configuration.Cloud.zkString, Configuration.Cloud.kafkaBrokers)
  }
}

/**
  * Responsible for launching/killing Kafka executors based on the current set of map/reduce queries
  * @param kafkaLocal
  * @param ec
  */
class QueryManager(kafkaLocal: KafkaConnection)(implicit ec: ExecutionContext) {
  type StreamExecutors = (Mapper[_, _, _, _], KVReducer[_,_])
  private val logger = LoggerFactory.getLogger(this.getClass)
  @volatile private var prevQueries: Map[Query[_,_], StreamExecutors] = Map()
  def updateQueries(newQueries: Iterable[Query[_,_]]): Future[Boolean] = {
    logger.info(s"Updating with queries $newQueries")
    val prevQuerySet = prevQueries.keySet
    val newQuerySet = newQueries.toSet
    val queriesToStop = prevQuerySet diff newQuerySet
    val stopFutures: Seq[Future[_]] = queriesToStop
      .flatMap{query =>
        val streamExecutors = prevQueries(query)
        logger.debug(s"Stopping queries $streamExecutors")
        Seq(streamExecutors._1.stop, streamExecutors._2.stop)
      }
      .toSeq
    val queriesToStart = newQuerySet diff prevQuerySet
    val newExecutors: Map[Query[_,_], StreamExecutors] = queriesToStart
      .map{query =>
        (query -> query.mkExecutors)
      }
      .toMap
    // Update queries map
    this.prevQueries = newExecutors
      .foldLeft(prevQueries.filterKeys(queriesToStop.contains(_) == false)){case (queries, queryMap) =>
        queries + queryMap
      }
    Future.sequence(stopFutures).map{_ =>
      newExecutors.values.foreach{case (m,r) =>
        //TODO: send canary messages
        logger.debug(s"Running $m $r")
        m.runAsync(ec)
        r.runAsync(ec)
        logger.debug(s"Started $m $r")
      }
    }
      .map(_ => logger.debug(s"Updated queries to $prevQueries by adding $newExecutors"))
      // Did anything change?
      .map(_ => (queriesToStart union queriesToStop).size > 0)
  }
}

class ClusterheadRelayManager(kafkaLocal: KafkaConnection)(implicit ec: ExecutionContext) {
  private val logger = LoggerFactory.getLogger(this.getClass)
  // Get clusterhead info from file
  private val clusterheadConnectionManager: ClusterheadConnectionManager = new StaticClusterheadConnectionManager()
  @volatile private var relayOption: Option[(KafkaConnectionInfo, ByteRelay)] = None
  // Reads from outputs of mappers
  private val inTopic = TopicLookupService.getMapperOutput
  // Stop relay if it exists
  private def stopRelay: Future[Unit] ={
    val stopFuture: Future[Unit] = relayOption match {
      case None => Future.successful{
        Unit
      }
      case Some((_, relay)) => relay.stop.map { _ =>
        Unit
      }
    }
    stopFuture
  }
  private def clusterheadUnchanged(newClusterhead: KafkaConnectionInfo): Boolean = {
    relayOption.map{case(oldClusterhead,_) =>
        newClusterhead.equals(oldClusterhead)
    }.getOrElse(false)
  }
  def updateRelay: Future[Unit] = {
    logger.debug(s"Updating relay")
    // Asynchronously start new relay
    val newRelayFuture = clusterheadConnectionManager.getClusterhead
      .map{newClusterhead =>
        clusterheadUnchanged(newClusterhead) match {
          case false => Some(newClusterhead)
          case true => None
        }
      }
    .map{
      case None => None
      case Some(newClusterhead) => {
        val newRelay = ByteRelay.mkClusterheadRelay(Set(inTopic), newClusterhead)
        // Stop old relay and start new one
        Some((newClusterhead, newRelay))
      }
    }
    newRelayFuture.map{
      case Some(relayTuple) => {
        val stopFuture: Future[_] = relayOption
          .map(_._2.stop.map(_ => Unit))
          .getOrElse(Future.successful[Unit](Unit))

        stopFuture.map { _ =>
          relayTuple._2.runAsync(ec)
          relayOption = Some(relayTuple)
          logger.debug(s"Updated clusterhead relay to $relayTuple")
          Unit
        }
      }
      case None => Future.successful(Unit)
    }

  }
}

class EventHandler(kafkaLocal: KafkaConnection, kafkaCloud: KafkaConnection) {
  private val logger = LoggerFactory.getLogger(this.getClass)
  import Configuration.Vehicle.{numIterations, iterationLengthMs}
  private val pool: ExecutorService = {
    val numCpus = Runtime.getRuntime.availableProcessors()
    val corePoolSize = numCpus * 40 //TODO: make configuration property
    val maxPoolSize = corePoolSize * 10 //TODO: make configuration property
    logger.info(s"Created pool of max size $maxPoolSize")
    val keepAliveMs = 0 //TODO: make configuration property
    val threadFactory = new ThreadFactory {
      private val threadNum = new AtomicLong()
      override def newThread(r: Runnable): Thread = {
        val thread = new Thread(r)
        thread.setName(s"EventThread-${threadNum.getAndIncrement()}")
        thread.setDaemon(false)
        thread
      }
    }
    val pool = new ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAliveMs, TimeUnit.MILLISECONDS, new LinkedBlockingQueue[Runnable](), threadFactory)
    pool.setRejectedExecutionHandler((r: Runnable, executor: ThreadPoolExecutor) => {
      logger.warn(s"Rejecting $r")
    })
    pool
  }
  private implicit val ec = ExecutionContext.fromExecutor(pool)
  private val queryLoader: QueryLoader = QueryLoader.getLoader
  private val executionCount = new AtomicLong(0)
  private val queryManager = new QueryManager(kafkaLocal)
  private val clusterheadRelayManager = new ClusterheadRelayManager(kafkaLocal)
  private val sensorProducer = SensorProducer.load(kafkaLocal)
  private val middlewareRelay = ByteRelay.mkMiddlewareRelay(ec)
  private val gossipEvent =
    GossipEvent.mkGossipEvent[String, (MPH, Long)](("customMsg", (123, 123)))

  private type Queries = Iterable[Query[_,_]]

  /**
    * Update active mappers/reducers if necessary
    * @param queries
    * @return true if mappers/reducers have changed
    */
  private def changeQueries(queries: Queries): Future[Option[Queries]] = {
    queryManager.updateQueries(queries).map{changed =>
      changed match {
        case false => None
        case true => Some(queries)
      }
    }
  }

  /**
    * The main event loop logic
    * @param timestamp
    * @return
    */
  private def executeInterval(timestamp: Timestamp): Future[Unit] ={
    val queryFuture = queryLoader.executeInterval(timestamp)
    //TODO: middleware uplink
    val queryChangeFuture: Future[Option[Queries]] = queryFuture.flatMap(changeQueries)
    val relayChangeFuture: Future[Unit] = queryChangeFuture.flatMap(_ => clusterheadRelayManager.updateRelay)
    val sensorProduceFuture: Future[Unit] = relayChangeFuture.flatMap(_ => sensorProducer.executeInterval(timestamp))
    /*
    val gossipFuture = gossipEvent.executeInterval(timestamp)
    val result = for {
      r1 <- sensorProduceFuture
      r2 <- gossipFuture
    } yield (r1 , r2)

    result.onComplete {
      case Success(x) => {
        logger.debug("success")
        Future.successful(Unit)
      }
      case Failure(x) => {
        logger.debug("failure")
        Future.successful(Unit)
      }
    }
    */
    Future.successful(Unit)
  }

  /**
    * Begin running main async event loop
    * @return
    */
  def start: Future[Unit] = {
    // Start middleware relay
    middlewareRelay.runAsync(ec)
    // Start scheduled threads
    val executor = Executors.newScheduledThreadPool(1)
    val completionPromise = Promise[Unit]()
    val runnable = new Runnable {
      private val logger = LoggerFactory.getLogger(this.getClass)
      override def run() = {
        logger.debug("Executing")
        Await.result(executeInterval(System.currentTimeMillis()), Duration.Inf)
        logger.debug("Completed iteration")
        if (executionCount.getAndIncrement() == numIterations) {
          logger.info("All iterations completed")
          completionPromise.success(Unit)
        }
      }
    }
    val initialDelay = 0//TODO: calculate based on current time modulo pre-set value s.t. all nodes start at roughly same time
    // How the main event loop is repeated
    val execFuture = executor.scheduleAtFixedRate(runnable, initialDelay, iterationLengthMs, TimeUnit.MILLISECONDS)
    completionPromise.future.map(_ => execFuture.cancel(true)).map(_ => executor.shutdownNow()).map(_ => Unit)
  }

  def stop: Unit = {
    pool.shutdownNow()
  }
}

/**
  * Created by wrkronmiller on 3/30/17.
  *
  * Entrypoint for on-vehicle softwware
  */
object Main {
  private val logger = LoggerFactory.getLogger(this.getClass)
  val SHUTDOWN_WAIT_MS = 2000
  private def shutdown(zkLocal: Connection[_],
                       kafkaLocal: Connection[_],
                       zkCloud: Connection[_],
                       kafkaCloud: Connection[_]) : Unit = {
    zkLocal.close
    kafkaLocal.close
    zkCloud.close
    kafkaCloud.close
    logger.info("Waiting to shut down")
    Thread.sleep(SHUTDOWN_WAIT_MS)
    logger.info("Shutting down")
  }
  private def getDebugMode: Boolean = {
    val debugStatus = System.getenv("DEBUG_VEHICLE")
    if(debugStatus == null) {
      false
    } else {
      debugStatus == "true"
    }
  }

  private val zkThread = new Runnable {
    override def run(): Unit = {
      val propsPath = "src/main/resources/zookeeper.props"
      assert((new File(propsPath)).exists, s"$propsPath does not exist")
      org.apache.zookeeper.server.quorum.QuorumPeerMain.main(Array(propsPath))
    }
  }

  private def startZkLocal: Unit = {
    Executors.newSingleThreadExecutor().submit(zkThread) //TODO: convert java future to scala future
  }

  private val kafkaThread = new Runnable {
    override def run(): Unit = {
      val propsPath = "src/main/resources/kafka.props"
      _root_.kafka.Kafka.main(Array(propsPath))
    }
  }

  private def startKafkaLocal: Unit ={
    Executors.newSingleThreadExecutor().submit(kafkaThread) //TODO: convert java future to scala future
  }

  def main(argv: Array[String]): Unit ={
    startZkLocal
    startKafkaLocal
      val (zkLocal, kafkaLocal) = StartupManager.waitLocal
      val (zkCloud, kafkaCloud) = StartupManager.waitCloud
      logger.info(s"Vehicle ${Configuration.Vehicle.nodeId} started: $zkLocal, $kafkaLocal, $zkCloud, $kafkaCloud")

      val eventHandler = new EventHandler(kafkaLocal, kafkaCloud)
      // Wait for event loop to terminate
      Await.result(eventHandler.start, Duration.Inf)
      // Shutdown
      eventHandler.stop
      shutdown(zkLocal, kafkaLocal, zkCloud, kafkaCloud)
      //TODO: clean shutdown
  }
}

