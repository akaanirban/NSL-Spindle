package edu.rpi.cs.nsl.spindle.vehicle.kafka.utils

import java.io.{File, PrintWriter}
import edu.rpi.cs.nsl.spindle.vehicle.Configuration.Vehicle.performanceLogPath

/**
  * Used to log performance data to CSV file
  * @param relayId
  * @param inTopics
  * @param outTopics
  */
class CSVMessageLogger(relayId: String, inTopics: Set[String], outTopics: Set[String]) extends MessageLogger(relayId, inTopics: Set[String], outTopics: Set[String]) {
  private def currentTime = System.currentTimeMillis()
  private def mkWriter(uniqueName: String, pathPrefix: String = performanceLogPath, pathSuffix: String = ".csv"): PrintWriter = {
    val path = s"$pathPrefix-$uniqueName-$currentTime$pathSuffix"
    val file = new File(path)
    if(file.exists){
      throw new RuntimeException(s"Failed to get unique file ${file.getAbsolutePath}")
    } else {
      val writer = new PrintWriter(file)
      writer.println(s"t,count\n$currentTime,0")
      writer.flush()
      writer
    }
  }
  private val csvLogPartialSuffix = s"-to-${outTopics.mkString("_")}-from-$relayId"
  //TODO: publish these metrics to shared kafka cluster
  private val sumWriter = mkWriter(s"relay-$relayId-sum")
  private val sizeWriter = mkWriter(s"relay-$relayId-size")

  private var sum: Long = 0
  override def logMessageSize(messageSize: Long) {
    sum += messageSize
    sumWriter.println(s"$currentTime,$sum")
    sizeWriter.println(s"$currentTime,$messageSize")
    sumWriter.flush()
    sizeWriter.flush()
  }

  override def close: Unit = {
    sumWriter.close
    sizeWriter.flush()
  }
}