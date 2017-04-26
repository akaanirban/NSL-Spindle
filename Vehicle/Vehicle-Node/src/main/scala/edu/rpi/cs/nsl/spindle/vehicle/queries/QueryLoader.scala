package edu.rpi.cs.nsl.spindle.vehicle.queries

import edu.rpi.cs.nsl.spindle.vehicle.Configuration
import edu.rpi.cs.nsl.spindle.vehicle.Types.Timestamp
import edu.rpi.cs.nsl.spindle.vehicle.events.TemporalDaemon
import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.ObjectSerializer
import edu.rpi.cs.nsl.spindle.vehicle.queries.testQueries.TestQueryLoader

import scala.concurrent.Future

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
  * QueryLoader factory
  */
object QueryLoader {
  /**
    * Choose loader based on whether hard-coded test queries are activated
    * @return QueryLoader
    */
  def getLoader: QueryLoader = {
    Configuration.Queries.testQueries match {
      //case None =>  //TODO: load queries from zookeeper
      case Some(testQueryStrings) => {
        val testQueries = TestQueryLoader.stringsToQueries(testQueryStrings)
        new MockQueryLoader(testQueries)
      }
    }
  }
}

//TODO: load queries from cloud zookeeper, come up with good implementation for loading queries from local cache (maybe just use local zookeeeper and have external program push to it)