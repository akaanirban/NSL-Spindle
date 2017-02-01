package edu.rpi.cs.nsl.spindle.vehicle.simulation

import edu.rpi.cs.nsl.spindle.datatypes._
import VehicleTypes._
import org.scalatest.FlatSpec
import edu.rpi.cs.nsl.spindle.vehicle.ReflectionUtils

class ReflectionUtilsSpec extends FlatSpec {

  it should "correctly generate a type tag string for vehicle types" in {
    val expectedString = "TypeTag[edu.rpi.cs.nsl.spindle.datatypes.VehicleTypes.MPH]"
    assert(ReflectionUtils.getTypeString[MPH] == expectedString)
    assert(ReflectionUtils.getTypeString[VehicleTypes.MPH] == expectedString)
    assert(ReflectionUtils.getTypeString[edu.rpi.cs.nsl.spindle.datatypes.VehicleTypes.MPH] == expectedString)
  }

  it should "correctly isolate objects of a particular type from a collection" in {
    import ReflectionFixtures.{ basicReadingCollection => collection }
    val mphResults = ReflectionUtils.getMatchingTypes(collection, ReflectionUtils.getTypeString[MPH])
    assert(mphResults.size == 1)
    assert(mphResults.last.value == ReflectionFixtures.mphValue)
  }
}