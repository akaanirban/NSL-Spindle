package edu.rpi.cs.nsl.spindle.vehicle.kafka.streams

import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.slf4j.LoggerFactory
import _root_.edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.ObjectSerializer
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Reducer
import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.streams.kstream.KGroupedStream
import scala.reflect.runtime.universe._

/**
 * Kafka streams mapper executor
 *
 * @see [[https://kafka.apache.org/0100/javadoc/org/apache/kafka/streams/kstream/KStream.html
 * #map(org.apache.kafka.streams.kstream.KeyValueMapper) Mapper Documentation]]
 */
class StreamMapper[K: TypeTag, V: TypeTag, K1: TypeTag, V1: TypeTag](inTopic: String,
                                 outTopic: String,
                                 mapFunc: (K, V) => (K1, V1),
                                 protected val config: StreamsConfig,
                                 startEpochOpt: Option[Long] = None)
    extends StreamExecutor(startEpochOpt) {
  private val logger = LoggerFactory.getLogger(s"StreamMapper $inTopic -> $outTopic")
  System.err.println(s"Created StreamMapper $inTopic -> $outTopic")
  protected val builder = {
    logger.debug(s"Configuring mapper builder for $inTopic to $outTopic")
    val builder = new KStreamBuilder
    val inStream: ByteStream = builder.stream(inTopic)
    val deserializedStream: KStream[K, V] = deserializeAndFilter(inStream)
    val mappedStream: KStream[K1, V1] = deserializedStream.map { (k, v) =>
      val (k1, v1) = mapFunc(k, v)
      //logger.debug(s"Mapping $k, $v -> $k1, $v1")
      System.err.println(s"Mapping $k, $v -> $k1, $v1")
      new KeyValue(k1, v1)
    }
    val serializedStream: ByteStream = serialize(mappedStream)
    writeOut(serializedStream, outTopic)
    builder
  }
}