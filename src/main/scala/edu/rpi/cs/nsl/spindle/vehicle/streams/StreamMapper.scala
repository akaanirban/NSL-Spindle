package edu.rpi.cs.nsl.spindle.vehicle.streams

import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.slf4j.LoggerFactory
import edu.rpi.cs.nsl.spindle.vehicle.kafka_utils.ObjectSerializer
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Reducer
import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.streams.kstream.KGroupedStream

/**
 * Kafka streams mapper executor
 *
 * @see [[https://kafka.apache.org/0100/javadoc/org/apache/kafka/streams/kstream/KStream.html#map(org.apache.kafka.streams.kstream.KeyValueMapper) Mapper Documentation]]
 */
class StreamMapper[K, V, K1, V1](inTopic: String, outTopic: String, mapFunc: (K, V) => (K1, V1), protected val config: StreamsConfig) extends StreamExecutor {
  private val logger = LoggerFactory.getLogger(this.getClass)
  protected val builder = {
    logger.debug(s"Configuring mapper builder for $inTopic to $outTopic")
    val builder = new KStreamBuilder
    val inStream: ByteStream = builder.stream(inTopic)
    val deserializedStream: KStream[K, V] = deserialize(inStream)
    val mappedStream: KStream[K1, V1] = deserializedStream.map { (k, v) =>
      val (k1, v1) = mapFunc(k, v)
      new KeyValue(k1, v1)
    }
    val serializedStream: ByteStream = serialize(mappedStream)
    writeOut(serializedStream, outTopic)
    builder
  }
}

/**
 * Perform ReduceByKey on stream
 */
class StreamKVReducer[K >: Null, V >: Null](inTopic: String, outTopic: String, reduceFunc: (V, V) => V, intermediateConfig: StreamsConfig) extends TypedStreamExecutor[K, V] {
  import scala.collection.JavaConverters._
  private val logger = LoggerFactory.getLogger(this.getClass)
  val config = {
    logger.debug("Setting default serde")
    val configMap = intermediateConfig.originals
    configMap.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, keySerde.getClass.getName)
    configMap.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, valueSerde.getClass.getName)
    System.err.println("Using map", configMap.asScala)
    new StreamsConfig(configMap)
  }
  private val reducer = new Reducer[V]() {
    def apply(value1: V, value2: V): V = {
      reduceFunc(value1, value2)
    }
  }
  private val reduceTableName = java.util.UUID.randomUUID.toString
  protected val builder = {
    logger.debug(s"Creating ReduceByKey builder for $inTopic to $outTopic")
    val builder = new KStreamBuilder
    val inStream: ByteStream = builder.stream(byteSerde, byteSerde, inTopic)
    val deserializedStream: KStream[K, V] = deserialize(inStream)
    val reducedStream: KStream[K, V] = deserializedStream
      .groupByKey
      .reduce(reducer, reduceTableName)
      .toStream
    val serializedStream: ByteStream = serialize(reducedStream) //TODO: fix
    writeOut(serializedStream, outTopic)
    builder
  }
}

//TODO: note - for a vanilla reduce, give everything the same key