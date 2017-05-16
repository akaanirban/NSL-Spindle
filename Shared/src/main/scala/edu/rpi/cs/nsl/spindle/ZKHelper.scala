package edu.rpi.cs.nsl.spindle

import org.I0Itec.zkclient.ZkClient
import org.slf4j.LoggerFactory
import edu.rpi.cs.nsl.spindle.datatypes.operations.Operation


trait QueryUidGenerator {
  def getQueryUid: String
}

/**
 * Helper class for performing common Zookeeper-related operations
 */
class ZKHelper(zkQuorum: String, topicName: String, queryUidGenerator: QueryUidGenerator) {
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
  private val path = mkPath(topicName)
  // Path to initial topic registration time
  private val datePath = s"$path/registerMs"
  // Path to lock (ensures one job per unique query)
  private val lockPath = s"$path/lock"
  // Path to query oplog
  private val queryPath = s"$path/query"
  // Path to query UID
  private val queryUidPath = s"$path/query-uid"

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

  private def mkQueryUid: String = {
    val queryUid = zkClient.exists(queryUidPath) match {
      case false => {
        val queryUid = queryUidGenerator.getQueryUid
        zkClient.createPersistent(queryUidPath, queryUid)
        queryUid
      }
      case true => zkClient.readData[String](queryUidPath)
    }

    queryUid
  }

  /**
   * Register a query for the current topic
   */
  def registerQuery(opLog: Seq[Operation[_, _]]): String = {
    val queryUid = mkQueryUid
    if (zkClient.exists(queryPath)) {
      logger.warn(s"Query already registered $topicName") //TODO: verify that oplogs match
    } else {
      logger.debug(s"Registering oplog $opLog")
      zkClient.createPersistent(queryPath, opLog)
    }
    queryUid
  }

  def close: Unit = zkClient.close
}
