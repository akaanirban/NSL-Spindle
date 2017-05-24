package edu.rpi.cs.nsl.spindle.vehicle.kafka.utils

import java.io.Closeable

/**
  * Created by wrkronmiller on 4/25/17.
  */
abstract class MessageLogger(relayId: String, inTopics: Set[String], outTopics: Set[String]) extends Closeable {
  def logMessageSize(messageSize: Long): Unit
}


object MessageLogger {
  def mkCsvLogger(relayId: String, inTopics: Set[String], outTopics: Set[String]): MessageLogger = {
    new CSVMessageLogger(relayId: String, inTopics: Set[String], outTopics: Set[String])
  }
}