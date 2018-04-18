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
object GossipQueryLoader {
  val gossipQueries: Map[String, Query[_, _]] = {
    val globalSpeedAvg: Query[_, _] = Query("globalSpeedAvg",
      MapOperation[(_, Vehicle), (_, (MPH, Long))](f=GossipMappers.getSpeedAndCount, uid="getSpeedAndCount"),
      ReduceByKeyOperation[(MPH, Long)](GossipReducers.sumSpeedAndCount, OperationIds.sum, uid="sumSpeedAndCount"))
    // Create map from query ID to query object
    Seq(globalSpeedAvg)
      .map(entry => (entry.id -> entry))
      .toMap
  }

  // Get a query by its string name using testQueries map (used in QueryLoader.scala)
  def stringsToQueries(strings: List[String]): List[Query[_,_]] = strings.map(gossipQueries(_))
}

object GossipReducers {
  def sumSpeedAndCount(a: (MPH, Long), b: (MPH, Long)): (MPH, Long) = {


    val logger = LoggerFactory.getLogger(this.getClass)
    logger.debug("reducing!")
    val gossipResult = GossipRunner.GetInstance().GetResult()
    val gossipResultParser = new GossipResultParser[MPH, Long](gossipResult)
    val results = gossipResultParser.GetResultWithDefault()

    val speed = results.getOrDefault("speed", 1.0).asInstanceOf[Double]
    val count = results.getOrDefault("count", 1.0).asInstanceOf[Double]

    logger.debug("done reducing, result is: {} {}", speed, count)

    (speed, Math.round(count))
  }
  //TODO: break into regions
  def getPosAndAccel(a: (MPH, Acceleration), b: (MPH, Acceleration)) = ???
}

object GossipMappers {
  def getSpeedAndCount(kv: (Any, Vehicle)): (String, (MPH, Long)) = {
    val (k,v) = kv
    ("speedAndCount", (v.mph, 1))
  }
  def getPosAndAccel(k: Any, v: Vehicle): (String, (MPH, Acceleration)) = {
    ("posAndAccel", (v.mph, v.acceleration))
  }
}