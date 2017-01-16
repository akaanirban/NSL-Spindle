package edu.rpi.cs.nsl.spindle.datatypes

import VehicleTypes._

object VehicleColors extends Enumeration {
  val red, orange, yellow, green, blue, purple, gray, white, black = Value
}

/**
 * Types for vehicle data
 *
 * @note used to find equivalent transformations
 */
object VehicleTypes {
  type VehicleId = Long
  type Lat = Double
  type Lon = Double
  type MPH = Double
}

/**
 * Base Case Class for Vehicle Data
 *
 * @todo rename to VehicleStatus (not VehicleMessage)
 *
 * @todo add maps for misc readings and properties
 */
case class Vehicle(id: VehicleId, lat: Lat,
                   lon: Lon, mph: MPH,
                   color: VehicleColors.Value)
