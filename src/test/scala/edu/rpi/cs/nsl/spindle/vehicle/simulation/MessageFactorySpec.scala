package edu.rpi.cs.nsl.spindle.vehicle.simulation

import edu.rpi.cs.nsl.spindle.datatypes.VehicleTypes
import org.scalatest.FlatSpec
import org.slf4j.LoggerFactory
import VehicleTypes._
import edu.rpi.cs.nsl.spindle.datatypes.VehicleColors

object ReflectionFixtures {
  val mphValue = 100
  val basicReadingCollection: Iterable[TypedValue[Any]] = Seq(TypedValue[MPH](mphValue), TypedValue[Lat](10), TypedValue[Lon](10)).asInstanceOf[Seq[TypedValue[Any]]]
  val basicPropertiesCollection: Iterable[TypedValue[Any]] = Seq(TypedValue[VehicleColors.Value](VehicleColors.red), TypedValue[VehicleId](0)).asInstanceOf[Seq[TypedValue[Any]]]
}

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

class MessageFactorySpec extends FlatSpec {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private object DummyMessageFactory extends VehicleMessageFactory {
    import scala.reflect.runtime.universe._
    def getValueOfTypeExposed[T: TypeTag](collection: Iterable[TypedValue[Any]]): T = getValueOfType[T](collection)
  }

  it should "correctly isolate object of specified type from a collection" in {
    import ReflectionFixtures.{ basicReadingCollection => collection }
    assert(DummyMessageFactory.getValueOfTypeExposed[MPH](collection) == ReflectionFixtures.mphValue)
  }

  it should "create a vehicle given a complete collection of readings and properties" in {
    import ReflectionFixtures.{ basicReadingCollection => readings, basicPropertiesCollection => properties }
    val vehicleMessage = VehicleMessageFactory.mkVehicle(readings, properties)
    assert(vehicleMessage.mph == ReflectionFixtures.mphValue)
  }

  it should "create a vehicle given extra data" in {
    import ReflectionFixtures.{ basicReadingCollection => readings, basicPropertiesCollection => properties }
    val augmentedReadings: Iterable[TypedValue[Any]] = readings ++ Seq(TypedValue[Int](1), TypedValue[Double](99)).asInstanceOf[Iterable[TypedValue[Any]]]
    val vehicleMessage = VehicleMessageFactory.mkVehicle(augmentedReadings, properties)
    assert(vehicleMessage.mph == ReflectionFixtures.mphValue)
  }
}