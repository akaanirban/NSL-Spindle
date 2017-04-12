package edu.rpi.cs.nsl.spindle.vehicle.queries.testQueries

import edu.rpi.cs.nsl.spindle.datatypes.Vehicle
import edu.rpi.cs.nsl.spindle.datatypes.VehicleTypes._
import edu.rpi.cs.nsl.spindle.datatypes.operations.{MapOperation, OperationIds, ReduceOperation}
import edu.rpi.cs.nsl.spindle.vehicle.queries.Query

/**
  * Created by wrkronmiller on 4/11/17.
  */
object TestQueryLoader {
  val testQueries: Map[String, Query[_, _]] = {
    Seq(Query("globalSpeedAvg",
      MapOperation[Vehicle, (_, (MPH, Long))](TestMappers.getSpeedAndCount),
      ReduceOperation[(MPH, Long),  (MPH, Long)](TestReducers.sumSpeedAndCount, OperationIds.sum)))
      .map(entry => (entry.id -> entry))
      .toMap
  }

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
  def getSpeedAndCount(v: Vehicle): (String, (MPH, Long)) = {
    ("speedAndCount", (v.mph, 1))
  }
  def getPosAndAccel(v: Vehicle): (String, (MPH, Acceleration)) = {
    ("posAndAccel", (v.mph, v.acceleration))
  }
}