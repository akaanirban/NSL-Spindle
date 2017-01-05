package edu.rpi.cs.nsl.spindle.vehicle.streams

import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.slf4j.LoggerFactory
import edu.rpi.cs.nsl.spindle.vehicle.kafka_utils.ObjectSerializer
import org.apache.kafka.streams.KeyValue

/**
 * Kafka streams mapper executor
 *
 * @see [[https://kafka.apache.org/0100/javadoc/org/apache/kafka/streams/kstream/KStream.html#map(org.apache.kafka.streams.kstream.KeyValueMapper) Mapper Documentation]]
 */
class StreamMapper[K, V, K1, V1](inTopic: String, outTopic: String, mapFunc: (K, V) => (K1, V1), streamsConfig: StreamsConfig) extends StreamExecutor {
  private val logger = LoggerFactory.getLogger(this.getClass)
  protected val config = streamsConfig
  protected val builder = {
    logger.debug(s"Configuring mapper builder for $inTopic to $outTopic")
    val builder = new KStreamBuilder
    val inStream: KStream[Array[Byte], Array[Byte]] = builder.stream(inTopic)
    val deserializedStream: KStream[K, V] = inStream.map { (k, v) => new KeyValue(ObjectSerializer.deserialize(k), ObjectSerializer.deserialize(v)) }
    val mappedStream: KStream[K1, V1] = deserializedStream.map { (k, v) =>
      val (k1, v1) = mapFunc(k, v)
      new KeyValue(k1, v1)
    }
    val serializedStream: KStream[ByteArray, ByteArray] = mappedStream.map { (k, v) => new KeyValue(ObjectSerializer.serialize(k), ObjectSerializer.serialize(v)) }
    serializedStream.to(byteSerde, byteSerde, outTopic)
    builder
  }
}

//TODO: note - for a vanilla reduce, give everything the same key