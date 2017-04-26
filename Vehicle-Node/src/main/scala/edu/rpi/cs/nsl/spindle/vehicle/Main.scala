package edu.rpi.cs.nsl.spindle.vehicle

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import edu.rpi.cs.nsl.spindle.vehicle.Types.Timestamp
import edu.rpi.cs.nsl.spindle.vehicle.connections.{Connection, KafkaConnection, Server, ZookeeperConnection}
import edu.rpi.cs.nsl.spindle.vehicle.events.SensorProducer
import edu.rpi.cs.nsl.spindle.vehicle.kafka.executors.{KVReducer, Mapper}
import edu.rpi.cs.nsl.spindle.vehicle.queries.{Query, QueryLoader}
import org.slf4j.LoggerFactory

import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
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

class QueryManager(kafkaLocal: KafkaConnection) {
  private val pool = Executors.newCachedThreadPool()
  type StreamExecutors = (Mapper[_, _, _, _], KVReducer[_,_])
  @volatile private var prevQueries: Map[Query[_,_], StreamExecutors] = Map()
  def updateQueries(newQueries: Iterable[Query[_,_]]): Future[Unit] = {
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
          m.runAsync(pool)
          r.runAsync(pool)
          println(s"Started $m $r")
      }
    }
      .map(_ => println(s"Updated queries to $prevQueries by adding $newExecutors"))
  }
}

class EventHandler(kafkaLocal: KafkaConnection, kafkaCloud: KafkaConnection) {
  import Configuration.Vehicle.{numIterations, iterationLengthMs}
  private val queryLoader: QueryLoader = QueryLoader.getLoader
  private val executionCount = new AtomicLong(0)
  private val queryManager = new QueryManager(kafkaLocal)
  private val sensorProducer = SensorProducer.load(kafkaLocal)
  private def executeInterval(timestamp: Timestamp): Future[Unit] ={
    val queryFuture = queryLoader.executeInterval(timestamp)
    //TODO: clusterhead relay
    //TODO: middleware uplink
    queryFuture
      .flatMap(queryManager.updateQueries)
      .map(_ => println("Updated queries"))
      .flatMap(_ => sensorProducer.executeInterval(timestamp))
      .map(_ => println("Sent sensor data"))
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
  def main(argv: Array[String]): Unit ={
    if(getDebugMode == false) {
      val (zkLocal, kafkaLocal) = StartupManager.waitLocal
      val (zkCloud, kafkaCloud) = StartupManager.waitCloud
      println(s"Vehicle ${Configuration.Vehicle.nodeId} started: $zkLocal, $kafkaLocal, $zkCloud, $kafkaCloud")

      val eventHandler = new EventHandler(kafkaLocal, kafkaCloud)
      eventHandler.start.map(_ => shutdown(zkLocal, kafkaLocal, zkCloud, kafkaCloud))
    }
  }
}

