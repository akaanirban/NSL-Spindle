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
import com.codahale.metrics.Counter
import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.Histogram
import com.codahale.metrics.JmxReporter
import com.codahale.metrics.SharedMetricRegistries
import java.util.Locale
import java.io.File
import com.codahale.metrics.CsvReporter
import java.util.concurrent.TimeUnit

class StreamRelay(inTopics: Set[String], outTopic: String, protected val config: StreamsConfig) extends StreamExecutor {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val deserializer = new ByteArrayDeserializer()
  private val serializer = new ByteArraySerializer()
  private val sourceName = "in-topics"
  private def uuid = java.util.UUID.randomUUID.toString
  private val metrics = SharedMetricRegistries.getOrCreate(s"metrics-$uuid")
  private val totalData: Counter = metrics.counter(s"data-sent-to-$outTopic-from-$uuid")
  private val dataHist: Histogram = metrics.histogram(s"message-sizes-to-$outTopic-from-$uuid")
  // Start reporting JMX metrics
  private val jmxReporter = {
    val reporter = JmxReporter.forRegistry(metrics).build
    reporter.start
    reporter
  }
  
  // Log to csv
  private val csvReporter = {
    val reporter = CsvReporter.forRegistry(metrics).formatFor(Locale.US).build(new File("simulation-results"))
    reporter.start(100, TimeUnit.MILLISECONDS)
    reporter
  }

  val builder = {
    val builder = new KStreamBuilder()
    val inStreams: Seq[ByteStream] = inTopics.toSeq.map(topic => builder.stream(topic): ByteStream)
    val mappedStreams: Seq[ByteStream] = inStreams.map { inStream =>
      val mappedStream: ByteStream = inStream.map { (k, v) =>
        logger.debug(s"Relaying message from $inStream to $outTopic")
        val messageSize = k.length + v.length
        totalData.inc(messageSize)
        dataHist.update(messageSize)
        new KeyValue[ByteArray, ByteArray](k, v)
      }
      mappedStream
    }
    mappedStreams.foreach(_.to(outTopic))
    logger.info(s"Relay created mapped topics $mappedStreams")
    builder
  }

  override def stopStream {
    super.stopStream
    jmxReporter.stop
    csvReporter.stop
  }
}