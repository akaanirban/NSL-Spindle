package edu.rpi.cs.nsl.spindle.vehicle

import java.io.File
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{Executors, TimeUnit}

import edu.rpi.cs.nsl.spindle.vehicle.Types.Timestamp
import edu.rpi.cs.nsl.spindle.vehicle.connections._
import edu.rpi.cs.nsl.spindle.vehicle.events.SensorProducer
import edu.rpi.cs.nsl.spindle.vehicle.kafka.KafkaQueryUtils._
import edu.rpi.cs.nsl.spindle.vehicle.kafka.executors.{ByteRelay, KVReducer, Mapper}
import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.TopicLookupService
import edu.rpi.cs.nsl.spindle.vehicle.queries.{Query, QueryLoader}
import org.slf4j.LoggerFactory

import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration.Duration

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

class QueryManager(kafkaLocal: KafkaConnection)(implicit ec: ExecutionContext) {
  type StreamExecutors = (Mapper[_, _, _, _], KVReducer[_,_])
  @volatile private var prevQueries: Map[Query[_,_], StreamExecutors] = Map()
  def updateQueries(newQueries: Iterable[Query[_,_]]): Future[Boolean] = {
    println(s"Updating with queries $newQueries")
    val prevQuerySet = prevQueries.keySet
    val newQuerySet = newQueries.toSet
    val queriesToStop = prevQuerySet diff newQuerySet
    val stopFutures: Seq[Future[_]] = queriesToStop
      .flatMap{query =>
        val streamExecutors = prevQueries(query)
        println(s"Stopping queries $streamExecutors")
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
        println(s"Running $m $r")
        m.runAsync(ec)
        r.runAsync(ec)
        println(s"Started $m $r")
      }
    }
      .map(_ => println(s"Updated queries to $prevQueries by adding $newExecutors"))
      // Did anything change?
      .map(_ => (queriesToStart union queriesToStop).size > 0)
  }
}

class ClusterheadRelayManager(kafkaLocal: KafkaConnection)(implicit ec: ExecutionContext) {

  // Get clusterhead info from file
  private val clusterheadConnectionManager: ClusterheadConnectionManager = new StaticClusterheadConnectionManager()
  @volatile private var relayOption: Option[ByteRelay] = None
  // Stop relay if it exists
  private def stopRelay: Future[Unit] ={
    val stopFuture: Future[Unit] = relayOption match {
      case None => Future.successful{
        Unit
      }
      case Some(relay) => relay.stop.map { _ =>
        Unit
      }
    }
    stopFuture
  }
  private def mkRelay(inTopics: Set[String]): Future[ByteRelay] = {
    println(s"Creating new relay $inTopics")
    clusterheadConnectionManager.getClusterhead
      .map{kafkaConnectionInfo =>
        println(s"Got clusterhead info $kafkaConnectionInfo")
        ByteRelay.mkRelay(inTopics, kafkaConnectionInfo)
      }
      .map{relay =>
        println(s"Starting relay $relay")
        relay.runAsync(ec)
        relay
      }
  }
  def updateRelay(newQueries: Iterable[Query[_,_]]): Future[Unit] = {
    println(s"Updating relay with queries $newQueries")
    // Get new list of topics
    val inTopics = newQueries.map(query => TopicLookupService.getMapperOutput(query.mapOperation.uid)).toSet
    // Asynchronously start new relay
    val mkRelayFuture: Future[ByteRelay] = mkRelay(inTopics)
    val stopRelayFuture: Future[ByteRelay] = mkRelayFuture.flatMap{newRelay =>
      println(s"Created new relay $newRelay")
      stopRelay.map(_=> newRelay)
    }
    stopRelayFuture.map{newRelay =>
      relayOption = Some(newRelay)
      Unit
    }
  }
}

class EventHandler(kafkaLocal: KafkaConnection, kafkaCloud: KafkaConnection) {
  import Configuration.Vehicle.{numIterations, iterationLengthMs}
  private val eventPool = Executors.newCachedThreadPool()
  private implicit val ec = ExecutionContext.fromExecutor(eventPool)
  private val queryLoader: QueryLoader = QueryLoader.getLoader
  private val executionCount = new AtomicLong(0)
  private val queryManager = new QueryManager(kafkaLocal)
  private val clusterheadRelayManager = new ClusterheadRelayManager(kafkaLocal)
  private val sensorProducer = SensorProducer.load(kafkaLocal)

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
    * Update clusterhead relay if active mappers/reducers have changed
    * @param queriesOption
    * @return
    */
  private def updateRelay(queriesOption: Option[Queries]): Future[Unit] = queriesOption match {
    case None => Future.successful(Unit)
    case Some(newQueries) => clusterheadRelayManager.updateRelay(newQueries)
  }

  private def executeInterval(timestamp: Timestamp): Future[Unit] ={
    val queryFuture = queryLoader.executeInterval(timestamp)
    //TODO: middleware uplink
    val queryChangeFuture: Future[Option[Queries]] = queryFuture.flatMap(changeQueries)
    val relayChangeFuture: Future[Unit] = queryChangeFuture.flatMap(updateRelay)
    val sensorProduceFuture: Future[Unit] = relayChangeFuture.flatMap(_ => sensorProducer.executeInterval(timestamp))
    sensorProduceFuture
  }
  def start: Future[Unit] = {
    val executor = Executors.newScheduledThreadPool(1)
    val completionPromise = Promise[Unit]()
    val runnable = new Runnable {
      private val logger = LoggerFactory.getLogger("EventHandlerThread")
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
    val execFuture = executor.scheduleAtFixedRate(runnable, initialDelay, iterationLengthMs, TimeUnit.MILLISECONDS)
    completionPromise.future.map(_ => execFuture.cancel(true)).map(_ => executor.shutdownNow()).map(_ => Unit)
  }

  def stop: Unit = {
    eventPool.shutdown()
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
    if(getDebugMode == false) {
      val (zkLocal, kafkaLocal) = StartupManager.waitLocal
      val (zkCloud, kafkaCloud) = StartupManager.waitCloud
      println(s"Vehicle ${Configuration.Vehicle.nodeId} started: $zkLocal, $kafkaLocal, $zkCloud, $kafkaCloud")

      val eventHandler = new EventHandler(kafkaLocal, kafkaCloud)
      // Wait for event loop to terminate
      Await.result(eventHandler.start, Duration.Inf)
      // Shutdown
      eventHandler.stop
      shutdown(zkLocal, kafkaLocal, zkCloud, kafkaCloud)
      //TODO: clean shutdown
    }
  }
}

