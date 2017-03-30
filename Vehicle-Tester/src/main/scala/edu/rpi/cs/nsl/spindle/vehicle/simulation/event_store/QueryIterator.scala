package edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store

import java.sql.ResultSet

abstract class QueryIterator[T](resultSet: ResultSet) extends Iterator[T] {
  def hasNext: Boolean = resultSet.next
}