package edu.rpi.cs.nsl.spindle.vehicle.connections
import scala.concurrent.{Future, blocking}
import scala.concurrent.duration.Duration
import edu.rpi.cs.nsl.spindle.vehicle.Configuration.Zookeeper.{connectTimeoutMs, sessionTimeoutMs}
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import scala.concurrent.duration._

import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by wrkronmiller on 4/5/17.
  */
class ZookeeperConnection(zkString: String, client: Option[ZkClient] = None) extends Connection[ZkClient]{

  override def close: ZookeeperConnection = {
    client match {
      case None => {}
      case Some(client) => client.close()
    }
    new ZookeeperConnection(zkString, None)
  }

  private def mkClient: ZkClient = {
    client match {
      case None => ZkUtils.createZkClient(zkString, sessionTimeoutMs, connectTimeoutMs)
      case _ => throw new AlreadyConnectedException()
    }
  }

  override def openSync(timeout: Duration): ZookeeperConnection = {
    val newClient = mkClient
    val waitInterval: Duration = timeout match {
      case Duration.Inf => (connectTimeoutMs milliseconds)
      case _ => timeout
    }
    newClient.waitUntilConnected(waitInterval._1, waitInterval._2)
    new ZookeeperConnection(zkString, Some(newClient))
  }

  override def openAsync: Future[ZookeeperConnection] = {
    Future {
      blocking {
        openSync()
      }
    }
  }

  override def getAdmin: ZkClient = {
    client match {
      case None => throw new RuntimeException("Not connected")
      case Some(zkClient) => zkClient
    }
  }
}

class AlreadyConnectedException extends RuntimeException {

}
