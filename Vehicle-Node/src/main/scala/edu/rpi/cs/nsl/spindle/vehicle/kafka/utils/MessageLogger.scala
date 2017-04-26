package edu.rpi.cs.nsl.spindle.vehicle.kafka.utils

import java.io.Closeable

/**
  * Created by wrkronmiller on 4/25/17.
  */
abstract class MessageLogger(inTopics: Set[String], outTopic: String) extends Closeable {
  def logMessageSize(messageSize: Long): Unit
}
