package edu.rpi.cs.nsl.spindle.vehicle

import edu.rpi.cs.nsl.spindle.vehicle.connections.{Connection, KafkaConnection, Server, ZookeeperConnection}
import edu.rpi.cs.nsl.spindle.vehicle.queries.QueryLoader
import org.slf4j.LoggerFactory
import scala.concurrent.ExecutionContext.Implicits.global

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
  def main(argv: Array[String]): Unit ={
    val (zkLocal, kafkaLocal) = StartupManager.waitLocal
    val (zkCloud, kafkaCloud) = StartupManager.waitCloud
    println(s"Vehicle started: $zkLocal, $kafkaLocal, $zkCloud, $kafkaCloud")

    val queryLoader: QueryLoader = QueryLoader.getLoader

    queryLoader.executeInterval(System.currentTimeMillis()).map{queries =>
      println(s"Got queries: $queries")
      //TODO
      shutdown(zkLocal, kafkaLocal, zkCloud, kafkaCloud)
    }
  }
}

