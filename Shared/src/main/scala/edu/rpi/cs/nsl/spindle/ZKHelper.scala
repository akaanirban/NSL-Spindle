package edu.rpi.cs.nsl.spindle

import org.I0Itec.zkclient.ZkClient
import org.slf4j.LoggerFactory
import edu.rpi.cs.nsl.spindle.datatypes.operations.Operation

/**
 * Helper class for performing common Zookeeper-related operations
 */
class ZKHelper(zkQuorum: String, topicName: String) {
  import Configuration.Zookeeper.{ registrationRoot, sessionTimeoutMS }

  val zkClient = new ZkClient(zkQuorum)

  val logger = LoggerFactory.getLogger(this.getClass)

  logger.debug("Waiting for ZK connection")
  zkClient.waitUntilConnected
  logger.debug("ZK connection online")

  /**
   * Create a persistent zkNode if it does not already exist
   */
  protected def mkIfNotExists(path: String) {
    val pathSep = "/"
    val pathComponents = path.split(pathSep)
    def mkNodeSafe(path: String) {
      if (path.length > 0 && zkClient.exists(path) == false) {
        zkClient.createPersistent(path)
      }
    }
    // Make path from top to bottom
    1.to(pathComponents.length).map { index =>
      mkNodeSafe { pathComponents.take(index).mkString(pathSep) }
    }
  }

  // Initialize registration root if necessary
  mkIfNotExists(registrationRoot)

  /**
   * Make complete path from registration root
   *
   * @param rel - relative path
   */
  protected def mkPath(rel: String): String = s"$registrationRoot/$rel"

  // Topic path
  val path = mkPath(topicName)
  // Path to initial topic registration time
  val datePath = s"$path/registerMs"
  // Path to lock (ensures one job per unique query)
  val lockPath = s"$path/lock"
  // Path to query oplog
  val queryPath = s"$path/query"

  /**
   * Register a Kafka topic in custom Zookeeper database
   */
  def registerTopic {
    logger.info(s"Registering topic $topicName")
    if (zkClient.exists(path)) {
      logger.warn(s"Topic already in zookeeper $topicName")
    } else {
      zkClient.createPersistent(path)
      zkClient.createPersistent(datePath, System.currentTimeMillis)
    }
    if (zkClient.exists(lockPath)) {
      throw new RuntimeException(s"Topic already locked $topicName")
    }
    zkClient.createEphemeral(lockPath)
  }

  /**
   * Register a query for the current topic
   */
  def registerQuery(opLog: Seq[Operation[_, _]]) {
    if (zkClient.exists(queryPath)) {
      logger.warn(s"Query already registered $topicName")
    } else {
      logger.debug(s"Registering oplog $opLog")
      zkClient.createPersistent(queryPath, opLog)
    }
  }

  def close: Unit = zkClient.close
}
