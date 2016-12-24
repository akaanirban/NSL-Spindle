package edu.rpi.cs.nsl.spindle.vehicle.simulation

/**
 * Simulator configuraiton settings
 *
 * @note not an object, so that it is easier to run unit tests and to run multiple tests in parallel
 */
case class Configuration(kafkaServers: String)