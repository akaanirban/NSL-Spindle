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
  type TempCentigrade = Double
  type Humidity = Double
  type TireDistance = Double
  type GroundDistance = Double
  type Acceleration = (Double, Double, Double)
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
                   color: VehicleColors.Value,
                   temperature: TempCentigrade,
                   humidity: Humidity,
                   tireDistance: TireDistance,
                   groundDistance: GroundDistance,
                   acceleration: Acceleration) {
  def this(m: Map[String, String]) = {
    this(m("id").toLong, m("lat").toDouble, m("lon").toDouble, m("mph").toDouble, VehicleColors.withName(m("color")),
      m("temperature").toDouble, m("humidity").toDouble, m("tireDistance").toDouble, m("groundDistance").toDouble,
      m("acceleration").split(",").map(_.toDouble) match { case Array(x,y,z) => (x,y,z)})
  }
}
