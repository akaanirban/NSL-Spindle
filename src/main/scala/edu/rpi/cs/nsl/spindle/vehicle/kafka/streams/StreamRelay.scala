package edu.rpi.cs.nsl.spindle.vehicle.kafka.streams

import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.processor.TopologyBuilder
import scala.collection.JavaConverters._
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.slf4j.LoggerFactory
import org.apache.kafka.streams.KeyValue
import java.util.concurrent.atomic.AtomicLong

class StreamRelay(inTopics: Set[String], outTopic: String, protected val config: StreamsConfig) extends StreamExecutor {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val deserializer = new ByteArrayDeserializer()
  private val serializer = new ByteArraySerializer()
  private val sourceName = "in-topics"
  private def uuid = java.util.UUID.randomUUID.toString
  val totalData: AtomicLong = new AtomicLong

  //TODO: use JMX
  val builder = {
    val builder = new KStreamBuilder()
    val inStreams: Seq[ByteStream] = inTopics.toSeq.map(topic => builder.stream(topic): ByteStream)
    val mappedStreams: Seq[ByteStream] = inStreams.map { inStream =>
      val mappedStream: ByteStream = inStream.map { (k, v) =>
        logger.debug(s"Relaying message from $inStream to $outTopic")
        logger.info(s"Sent ${totalData.getAndAdd(v.length + k.length)} total bytes  to $outTopic")
        new KeyValue[ByteArray, ByteArray](k, v)
      }
      mappedStream
    }
    mappedStreams.foreach(_.to(outTopic))
    logger.info(s"Relay created mapped topics $mappedStreams")
    builder
  }

}