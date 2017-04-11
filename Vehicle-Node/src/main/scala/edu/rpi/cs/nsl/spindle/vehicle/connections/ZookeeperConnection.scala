package edu.rpi.cs.nsl.spindle.vehicle.connections
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.Duration
import edu.rpi.cs.nsl.spindle.vehicle.Configuration.Zookeeper.{connectTimeoutMs, sessionTimeoutMs}
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.apache.zookeeper.Watcher.Event
import org.apache.zookeeper.{WatchedEvent, Watcher}
import org.slf4j.LoggerFactory

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
    newClient.waitUntilConnected(timeout._1, timeout._2)
    new ZookeeperConnection(zkString, Some(newClient))
  }

  override def openAsync: Future[ZookeeperConnection] = {
    val newClient = mkClient
    val connectionPromise = Promise[ZookeeperConnection]()
    newClient.connect(connectTimeoutMs, new Watcher{
      private val logger = LoggerFactory.getLogger(this.getClass)
      override def process(event: WatchedEvent) = {
        event.getState match {
          case Event.KeeperState.SyncConnected => {
            logger.info(s"ZK Watcher for $zkString connected")
            connectionPromise.success(new ZookeeperConnection(zkString, Some(newClient)))
          }
          case state => logger.debug(s"Zk Watcher for $zkString received event: ${state}")
        }
      }
    })
    connectionPromise.future
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
