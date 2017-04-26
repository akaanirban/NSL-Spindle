package edu.rpi.cs.nsl.spindle.middleware

import java.io.Closeable
import java.util

import akka.actor.{ActorRef, ActorSystem}
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.IZkChildListener
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

/**
  * Created by wrkronmiller on 3/7/17.
  */
object Core extends MiddlewareCore {
  def main(args: Array[String]): Unit ={
    //TODO
  }
}

trait MiddlewareCore {
  protected val actorSystem = ActorSystem("SpindleMiddleware")
}

trait ZkClient extends Closeable {
  import Configuration.ZkPaths
  private val (client, connection) = ZkUtils.createZkClientAndConnection(Configuration.zkString,
    sessionTimeout=Configuration.zkSessionTimeout,
    connectionTimeout = Configuration.zkConnectionTimeout)

  def initDirs: Unit = {
    val paths = Seq(ZkPaths.root, ZkPaths.queries)
    paths
      .filterNot(client.exists)
      .foreach(client.createPersistent(_))
    assert(paths.forall(client.exists), s"Failed to create all paths $paths")

  }

  def getQueries: List[String] = {
    client.getChildren(ZkPaths.queries).toList
  }

  def watchQueries(queryWatcher: QueryWatcher): Unit = {
    client.subscribeChildChanges(ZkPaths.queries, queryWatcher)
  }

  def addQuery(query: Any): Unit = { //TODO: figure out a type for query
    client.createEphemeralSequential(ZkPaths.queries, query)
  }

  def close: Unit = {
    client.close()
    connection.close()
  }
}

// Query manager actor
object QueryManager {
  case class UpdateQueries(queryNodes: Iterable[String])
}

class QueryWatcher(queryManager: ActorRef) extends IZkChildListener {
  private val logger = LoggerFactory.getLogger(this.getClass)
  override def handleChildChange(parentPath: String, currentChilds: util.List[String]): Unit = {
    val querySet = currentChilds.toSet
    queryManager ! QueryManager.UpdateQueries(querySet)
    logger.debug(s"Sent queries $querySet to $queryManager")
  }
}