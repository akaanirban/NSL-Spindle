package edu.rpi.cs.nsl.spindle.vehicle.simulation

import edu.rpi.cs.nsl.spindle.datatypes._
import VehicleTypes._

object ReflectionFixtures {
  val mphValue = 100
  val basicReadingCollection: Iterable[TypedValue[Any]] = Seq(TypedValue[MPH](mphValue), TypedValue[Lat](10), TypedValue[Lon](10)).asInstanceOf[Seq[TypedValue[Any]]]
  val basicPropertiesCollection: Iterable[TypedValue[Any]] = Seq(TypedValue[VehicleColors.Value](VehicleColors.red), TypedValue[VehicleId](0)).asInstanceOf[Seq[TypedValue[Any]]]
}