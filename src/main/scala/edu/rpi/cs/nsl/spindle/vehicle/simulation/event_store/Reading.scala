package edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store

case class Reading(timestamp: Double, node: Int, reading: Double)
