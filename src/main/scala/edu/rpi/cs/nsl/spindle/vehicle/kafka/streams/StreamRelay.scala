package edu.rpi.cs.nsl.spindle.vehicle.kafka.streams

import java.io.{Closeable, File, FileOutputStream, PrintWriter}

import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.slf4j.LoggerFactory
import _root_.edu.rpi.cs.nsl.spindle.vehicle.simulation.Configuration
import _root_.edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.ObjectSerializer
import _root_.edu.rpi.cs.nsl.spindle.vehicle.TypedValue

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

//TODO: filter by start epoch
class StreamRelay(relayId: String,
                   inTopics: Set[String],
                  outTopic: String,
                  protected val config: StreamsConfig,
                  startEpoch: Long = System.currentTimeMillis()) extends StreamExecutor(readableId = s"Relay:${inTopics.mkString(",") }->$outTopic"){
  private val logger = LoggerFactory.getLogger(s"Stream Relay $inTopics -> $outTopic")
  private val messageLogger = new CSVMessageLogger(relayId, inTopics, outTopic)


  logger.debug(s"Creating stream relay from ${inTopics} -> $outTopic")

  val builder = {
    val builder = new KStreamBuilder()
    val inStreams: Seq[ByteStream] = inTopics.toSeq.map(topic => builder.stream(topic): ByteStream)
    val filteredStreams: Seq[ByteStream] = inStreams.map { inStream =>
      val filteredStream: ByteStream = inStream
      .filterNot{(k,v) => k == null || v == null}
      .filterNot { (k, v) =>
        logger.debug(s"Relaying message from $inStream to $outTopic")
        val messageSize = k.length + v.length
        val deserializedKey = ObjectSerializer.deserialize[TypedValue[Any]](k)
        val reject = deserializedKey.isCanary || deserializedKey.creationEpoch < startEpoch
        logger.trace(s"Relaying message from $inTopics to $outTopic: ($k, $v) - reject $reject")
        if(reject == false) {
          messageLogger.logMessageSize(messageSize)
        }
        reject
      }
      filteredStream
    }
    filteredStreams.foreach(_.to(outTopic))
    logger.info(s"Relay created mapped topics $filteredStreams")
    builder
  }

  override def stopStream: Future[Any] = {
    super.stopStream.map {_ =>
      val message = s"Stopped stream relay $inTopics -> $outTopic"
      logger.info(message)
      System.out.println(message)
      messageLogger.close()
      None
    }
  }
}

abstract class MessageLogger(inTopics: Set[String], outTopic: String) extends Closeable {
  def logMessageSize(messageSize: Long): Unit
}

//TODO: log directly to database

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
  private val sumWriter = mkWriter(s"${Configuration.simResultsDir}/data-sent$csvLogPartialSuffix")
  private val sizeWriter = mkWriter(s"${Configuration.simResultsDir}/message-size$csvLogPartialSuffix")

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