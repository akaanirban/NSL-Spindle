package edu.rpi.cs.nsl.spindle.vehicle.connections

import edu.rpi.cs.nsl.spindle.vehicle.Configuration
import edu.rpi.cs.nsl.spindle.vehicle.kafka.executors.KafkaConnectionInfo
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}

/**
  * Get the current clusterhead address
  */
trait ClusterheadConnectionManager {
  def getClusterhead(implicit ec: ExecutionContext): Future[KafkaConnectionInfo]
}

/**
  * Get clusterhead from config file
  */
class StaticClusterheadConnectionManager extends ClusterheadConnectionManager{
  import Configuration.Vehicle.{clusterheadZkString, clusterheadBroker}
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val connectionInfo = KafkaConnectionInfo(clusterheadZkString, clusterheadBroker)
  override def getClusterhead(implicit ec: ExecutionContext): Future[KafkaConnectionInfo] = {
    Future.successful(connectionInfo).map{connectionInfo =>
      logger.debug(s"Sending $connectionInfo")
      connectionInfo
    }
  }
}

//TODO: for simulation get clusterhead from central source such as zookeeper - implement a new subclass of ClusterheadConnectionManager and instantiate it in Main.scala