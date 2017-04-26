package edu.rpi.cs.nsl.v2v.operations

import edu.rpi.cs.nsl.v2v.spark.NSLSpec
import edu.rpi.cs.nsl.spindle.datatypes.Vehicle
import edu.rpi.cs.nsl.spindle.datatypes.operations.OperationIds
import edu.rpi.cs.nsl.spindle.datatypes.operations.MapOperation
import edu.rpi.cs.nsl.spindle.datatypes.operations.ReduceOperation

class OperationSpec extends NSLSpec {
  import OperationIds._
  import edu.rpi.cs.nsl.spindle.datatypes.VehicleTypes._
  it should "initialize Map and Reduce Operations without error" in {
    // No-op
    val mapOp = MapOperation[Any, Any](a => a)
    // Fake min operation
    val reduceOp = ReduceOperation[Any, Any]((a, b) => a, min)
  }
  trait BasicOperations {
    val mapNop = MapOperation[Any, Any](a => a)
    val mapDouble = MapOperation[Double, Double](a => Math.pow(a, 2))
    val mapInt = MapOperation[Int, Int](a => a * a)
    val mapDoubleInt = MapOperation[Double, Int](a => Math.round(a).toInt)
    val mapVehicleDouble = MapOperation[Vehicle, Double](_.mph)

    lazy val maps = Set(mapNop, mapDouble, mapInt, mapDoubleInt, mapVehicleDouble)

    // Reducers
    val sumDouble = ReduceOperation[Double, Double](_ + _, sum)
    val minDouble = ReduceOperation[Double, Double]((a, b) => Math.min(a, b), min)
    val maxDouble = ReduceOperation[Double, Double]((a, b) => Math.max(a, b), max)

    lazy val reduces = Set(sumDouble, minDouble, maxDouble)
  }
  
  it should "be able to tell map from reduce" in new BasicOperations {
    assert {
      maps.forall { map =>
        reduces.forall(_.equals(map) == false)
      }
    }
  }

  it should "correctly identify equal map operations" in new BasicOperations {
    // Check identity
    assert { maps.forall { map => map.equals(map) } }
    // Check inequality
    assert {
      maps.forall { map =>
        maps.diff(Set(map)).forall(_.equals(map) == false)
      }
    }

    //Check equivalence
    val inequivOne = MapOperation[Vehicle, MPH](_.mph)
    val inequivTwo = MapOperation[Vehicle, Lat](_.lat)
    assert(inequivOne.equals(inequivTwo) == false)

    val equivOne = MapOperation[Vehicle, MPH](_.mph)
    val equivTwo = MapOperation[Vehicle, MPH](_.mph)
    assert(equivOne.equals(equivTwo))
    assert(equivOne.hashCode == equivTwo.hashCode)
  }
}