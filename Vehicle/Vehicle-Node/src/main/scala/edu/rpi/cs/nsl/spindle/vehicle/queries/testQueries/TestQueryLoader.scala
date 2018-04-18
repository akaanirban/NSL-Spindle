package edu.rpi.cs.nsl.spindle.vehicle.queries.testQueries

import edu.rpi.cs.nsl.spindle.datatypes.Vehicle
import edu.rpi.cs.nsl.spindle.datatypes.VehicleTypes._
import edu.rpi.cs.nsl.spindle.datatypes.operations.{MapOperation, OperationIds, ReduceByKeyOperation}
import edu.rpi.cs.nsl.spindle.vehicle.gossip.GossipRunner
import edu.rpi.cs.nsl.spindle.vehicle.gossip.results.GossipResultParser
import edu.rpi.cs.nsl.spindle.vehicle.queries.Query
import org.slf4j.LoggerFactory

/**
  * Created by wrkronmiller on 4/11/17.
  */
object TestQueryLoader {
  val testQueries: Map[String, Query[_, _]] = {
    val globalSpeedAvg: Query[_, _] = Query("globalSpeedAvg",
      MapOperation[(_, Vehicle), (_, (MPH, Long))](f=TestMappers.getSpeedAndCount, uid="getSpeedAndCount"),
      ReduceByKeyOperation[(MPH, Long)](TestReducers.sumSpeedAndCount, OperationIds.sum, uid="sumSpeedAndCount"))
    // Create map from query ID to query object
    Seq(globalSpeedAvg)
      .map(entry => (entry.id -> entry))
      .toMap
  }

  // Get a query by its string name using testQueries map (used in QueryLoader.scala)
  def stringsToQueries(strings: List[String]): List[Query[_,_]] = strings.map(testQueries(_))
}

object TestReducers {
  def sumSpeedAndCount(a: (MPH, Long), b: (MPH, Long)): (MPH, Long) = {
    (a._1 + b._1, a._2 + b._2)
  }
  //TODO: break into regions
  def getPosAndAccel(a: (MPH, Acceleration), b: (MPH, Acceleration)) = ???
}

object TestMappers {
  def getSpeedAndCount(kv: (Any, Vehicle)): (String, (MPH, Long)) = {
    val (k,v) = kv
    ("speedAndCount", (v.mph, 1))
  }
  def getPosAndAccel(k: Any, v: Vehicle): (String, (MPH, Acceleration)) = {
    ("posAndAccel", (v.mph, v.acceleration))
  }
}