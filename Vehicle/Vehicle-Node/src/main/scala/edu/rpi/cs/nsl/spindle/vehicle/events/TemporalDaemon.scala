package edu.rpi.cs.nsl.spindle.vehicle.events

import edu.rpi.cs.nsl.spindle.vehicle.Types.Timestamp

import scala.concurrent.Future

/**
  * Created by wrkronmiller on 4/11/17.
  */
trait TemporalDaemon[T] {
  def executeInterval(currentTime: Timestamp): Future[T]
  def safeShutdown: Future[Unit]
  def fullShutdown: Future[Unit] = safeShutdown
}
