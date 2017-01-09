package edu.rpi.cs.nsl.spindle.vehicle.kafka.streams

import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.slf4j.LoggerFactory
import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.ObjectSerializer
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Reducer
import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.streams.kstream.KGroupedStream

/**
 * Perform ReduceByKey on stream
 */
class StreamKVReducer[K >: Null, V >: Null](inTopic: String, outTopic: String, reduceFunc: (V, V) => V, intermediateConfig: StreamsConfig) extends TypedStreamExecutor[K, V] {
  private val logger = LoggerFactory.getLogger(this.getClass)
  protected val config = {
    logger.debug("Setting default serde")
    val configMap = intermediateConfig.originals
    configMap.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, keySerde.getClass.getName)
    configMap.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, valueSerde.getClass.getName)
    new StreamsConfig(configMap)
  }
  protected val reducer = new Reducer[V]() {
    def apply(value1: V, value2: V): V = {
      reduceFunc(value1, value2)
    }
  }
  protected def mkRandTopic = java.util.UUID.randomUUID.toString
  protected val reduceTableName = mkRandTopic
  protected val builder = {
    logger.debug(s"Creating ReduceByKey builder for $inTopic to $outTopic")
    val builder = new KStreamBuilder
    val inStream: ByteStream = builder.stream(byteSerde, byteSerde, inTopic)
    val deserializedStream: KStream[K, V] = deserialize(inStream)
    val reducedStream: KStream[K, V] = deserializedStream
      .groupByKey
      .reduce(reducer, reduceTableName)
      .toStream
    val serializedStream: ByteStream = serialize(reducedStream)
    writeOut(serializedStream, outTopic)
    builder
  }
}