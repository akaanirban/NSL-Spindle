package edu.rpi.cs.nsl.spindle.vehicle.kafka.streams

import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.processor.TopologyBuilder
import scala.collection.JavaConverters._
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.slf4j.LoggerFactory
import org.apache.kafka.streams.KeyValue

class StreamRelay(inTopics: Set[String], outTopic: String, protected val config: StreamsConfig) extends StreamExecutor {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val deserializer = new ByteArrayDeserializer()
  private val serializer = new ByteArraySerializer()
  private val sourceName = "in-topics"
  private def uuid = java.util.UUID.randomUUID.toString

  val builder = {
    val builder = new KStreamBuilder()
    val inStreams: Seq[ByteStream] = inTopics.toSeq.map(topic => builder.stream(topic): ByteStream)
    val mappedStreams: Seq[ByteStream] = inStreams.map { inStream =>
      inStream.map { (k, v) =>
        logger.debug(s"Relaying message from $inStream to $outTopic")
        new KeyValue(k, v)
      }
    }
    mappedStreams.foreach(_.to(outTopic))
    logger.info(s"Relay created mapped topics $mappedStreams")
    builder
  }

}