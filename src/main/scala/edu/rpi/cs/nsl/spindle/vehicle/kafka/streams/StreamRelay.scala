package edu.rpi.cs.nsl.spindle.vehicle.kafka.streams

import java.io.File

import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.slf4j.LoggerFactory
import com.codahale.metrics.Counter
import com.codahale.metrics.Histogram
import com.codahale.metrics.JmxReporter
import com.codahale.metrics.SharedMetricRegistries
import java.util.Locale

import _root_.edu.rpi.cs.nsl.spindle.vehicle.simulation.Configuration
import _root_.edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.ObjectSerializer
import _root_.edu.rpi.cs.nsl.spindle.vehicle.TypedValue
import com.codahale.metrics.CsvReporter
import java.util.concurrent.TimeUnit

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

//TODO: filter by start epoch
class StreamRelay(inTopics: Set[String],
                  outTopic: String,
                  protected val config: StreamsConfig,
                  startEpoch: Long = System.currentTimeMillis()) extends StreamExecutor {
  private val logger = LoggerFactory.getLogger(s"Stream Relay $inTopics -> $outTopic")
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
    val reporter = CsvReporter.forRegistry(metrics).formatFor(Locale.US).build(new File(Configuration.simResultsDir))
    reporter.start(Configuration.simReportSeconds, TimeUnit.SECONDS)
    reporter
  }

  System.err.println(s"Creating stream relay from ${inTopics} -> $outTopic")

  val builder = {
    val builder = new KStreamBuilder()
    val inStreams: Seq[ByteStream] = inTopics.toSeq.map(topic => builder.stream(topic): ByteStream)
    val filteredStreams: Seq[ByteStream] = inStreams.map { inStream =>
      val filteredStream: ByteStream = inStream.filterNot { (k, v) =>
        logger.debug(s"Relaying message from $inStream to $outTopic")
        val messageSize = k.length + v.length
        val deserializedKey = ObjectSerializer.deserialize[TypedValue[Any]](k)
        val reject = deserializedKey.isCanary || deserializedKey.creationEpoch < startEpoch
        System.err.println(s"Relaying message from $inTopics to $outTopic: ($k, $v) - reject $reject")
        if(reject == false) {
          totalData.inc(messageSize)
          dataHist.update(messageSize)
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
    csvReporter.stop()
    csvReporter.close()
    logger.info(s"Shut down reporters for $inTopics -> $outTopic")
    super.stopStream.map {_ =>
      jmxReporter.stop()
      jmxReporter.close()
      val message = s"Stopped stream relay $inTopics -> $outTopic"
      logger.info(message)
      None
    }
  }
}