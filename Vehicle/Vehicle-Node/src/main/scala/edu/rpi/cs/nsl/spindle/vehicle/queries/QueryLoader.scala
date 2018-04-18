package edu.rpi.cs.nsl.spindle.vehicle.queries

import edu.rpi.cs.nsl.spindle.ZKHelper
import edu.rpi.cs.nsl.spindle.vehicle.Configuration
import edu.rpi.cs.nsl.spindle.vehicle.Types.Timestamp
import edu.rpi.cs.nsl.spindle.vehicle.events.TemporalDaemon
import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.ObjectSerializer
import edu.rpi.cs.nsl.spindle.vehicle.queries.testQueries.{TestQueryLoader, GossipQueryLoader}
import org.I0Itec.zkclient.ZkClient

import scala.concurrent.{ExecutionContext, Future, blocking}

/**
  * Created by wrkronmiller on 4/11/17.
  *
  * Load query for current timestamp
  */
trait QueryLoader extends TemporalDaemon[Iterable[Query[_,_]]]

/**
  * Simulate remote query store
  * @param queries
  */
class MockQueryLoader(queries: List[Query[_, _]]) extends QueryLoader {
  override def safeShutdown: Future[Unit] = Future.successful(Unit)
  override def executeInterval(_currentTime: Timestamp): Future[Iterable[Query[_,_]]] = {
    // LocalSerDe to ensure queries really are serializable
    val serializedQueries = ObjectSerializer.serialize(queries.map(ObjectSerializer.serialize))
    Future.successful{
      ObjectSerializer.deserialize[List[Array[Byte]]](serializedQueries)
        .map(ObjectSerializer.deserialize[Query[_,_]])
    }
  }
}

/**
  * Load configured queries from local zookeeper
  *
  * @note this expects some other program running on the local machine to be managing the set of active queries
  */
class ZookeeperQueryLoader(implicit ec: ExecutionContext) extends QueryLoader {
  private val ACTIVE_QUERIES_NODE = "/queries/active"
  private val zkClient = new ZkClient(Configuration.Local.zkString)
  //TODO: need some other client running on local device to update local zookeeper with current set of active queries
  override def executeInterval(currentTime: Timestamp): Future[Iterable[Query[_, _]]] = {
    if(zkClient.exists(ACTIVE_QUERIES_NODE)){
      val queriesBytes = zkClient.readData[Array[Byte]](ACTIVE_QUERIES_NODE)
      Future{
        blocking{
          ObjectSerializer.deserialize[List[Array[Byte]]](queriesBytes).map { queryBytes =>
            ObjectSerializer.deserialize[Query[_,_]](queryBytes)
          }
        }
      }
    } else {
      // If ZK Node is missing, return empty list of queries
      Future.successful[List[Query[_,_]]](List())
    }
  }

  override def safeShutdown: Future[Unit] = Future{blocking{zkClient.close}}
}

/**
  * QueryLoader factory
  */
object QueryLoader {
  /**
    * Choose loader based on whether hard-coded test queries are activated
    * @return QueryLoader
    */
  def getLoader(implicit ec: ExecutionContext): QueryLoader = {
    Configuration.Queries.testQueries match {
      case None =>  new ZookeeperQueryLoader()
      case Some(testQueryStrings) => {
        val testQueries = TestQueryLoader.stringsToQueries(testQueryStrings)
//        val testQueries = GossipQueryLoader.stringsToQueries(testQueryStrings)
        new MockQueryLoader(testQueries)
      }
    }
  }
}