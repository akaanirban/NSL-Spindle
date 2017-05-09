package edu.rpi.cs.nsl.spindle.vehicle.connections

import edu.rpi.cs.nsl.spindle.vehicle.Configuration
import edu.rpi.cs.nsl.spindle.vehicle.kafka.executors.KafkaConnectionInfo

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
  private val connectionInfo = KafkaConnectionInfo(clusterheadZkString, clusterheadBroker)
  override def getClusterhead(implicit ec: ExecutionContext): Future[KafkaConnectionInfo] = {
    Future.successful(connectionInfo).map{connectionInfo =>
      println(s"Sending $connectionInfo")
      connectionInfo
    }
  }
}