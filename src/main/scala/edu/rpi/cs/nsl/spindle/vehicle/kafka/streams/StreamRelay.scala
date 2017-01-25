package edu.rpi.cs.nsl.spindle.vehicle.kafka.streams

import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.processor.TopologyBuilder
import scala.collection.JavaConverters._
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer

class StreamRelay(inTopics: Set[String], outTopics: Set[String], protected val config: StreamsConfig) extends StreamExecutor {
  private val deserializer = new ByteArrayDeserializer()
  private val serializer = new ByteArraySerializer()
  private val sourceName = "in-topics"
  private def uuid = java.util.UUID.randomUUID.toString
  val builder = outTopics
    .foldLeft(new TopologyBuilder().addSource(sourceName, deserializer, deserializer, inTopics.toSeq: _*))((builder, outTopic) =>
      builder.addSink(uuid, outTopic, serializer, serializer, sourceName))
}