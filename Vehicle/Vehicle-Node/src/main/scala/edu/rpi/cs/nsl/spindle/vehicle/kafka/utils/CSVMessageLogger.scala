package edu.rpi.cs.nsl.spindle.vehicle.kafka.utils

import java.io.{File, PrintWriter}

/**
  * Used to log performance data to CSV file
  * @param relayId
  * @param inTopics
  * @param outTopic
  */
class CSVMessageLogger(relayId: String, inTopics: Set[String], outTopic: String) extends MessageLogger(inTopics: Set[String], outTopic: String) {
  private def currentTime = System.currentTimeMillis()
  private def getNewPath(pathPrefix: String, pathSuffix: String, appendNum: Long = 0): String = {
    val testPath = s"$pathPrefix-$appendNum$pathSuffix"
    if(new File(testPath).exists){
      getNewPath(pathPrefix, pathSuffix, appendNum + 1)
    } else {
      testPath
    }
  }
  private def mkWriter(pathPrefix: String, pathSuffix: String = ".csv"): PrintWriter = {
    val file = new File(getNewPath(pathPrefix, pathSuffix))
    if(file.exists){
      throw new RuntimeException(s"Failed to get unique file ${file.getAbsolutePath}")
    } else {
      val writer = new PrintWriter(file)
      writer.println(s"t,count\n$currentTime,0")
      writer.flush()
      writer
    }
  }
  private val csvLogPartialSuffix = s"-to-$outTopic-from-$relayId"
  //TODO: publish these metrics to shared kafka cluster
  private val sumWriter = System.out//mkWriter(s"${Configuration.simResultsDir}/data-sent$csvLogPartialSuffix")
  private val sizeWriter = System.out//mkWriter(s"${Configuration.simResultsDir}/message-size$csvLogPartialSuffix")

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