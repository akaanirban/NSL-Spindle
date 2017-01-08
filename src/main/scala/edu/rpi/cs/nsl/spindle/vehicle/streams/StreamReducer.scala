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
import org.apache.kafka.streams.kstream.KeyValueMapper

/**
 * Perform pure reduce on stream
 *
 * @note - strips keys
 *
 * @see [[https://github.com/confluentinc/examples/blob/10eafe95a972bf18b9f681129404044ef36ee8cc/kafka-streams/src/main/java/io/confluent/examples/streams/SumLambdaExample.java#L116 Confluent Reduce Example]]
 */
class StreamReducer[K >: Null, V >: Null](inTopic: String, outTopic: String, reduceFunc: (V, V) => V, intermediateConfig: StreamsConfig) extends StreamKVReducer[K, V](inTopic: String, outTopic: String, reduceFunc: (V, V) => V, intermediateConfig: StreamsConfig) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  override protected val builder = {
    val builder = new KStreamBuilder
    val deserializedStream: KStream[K, V] = deserialize {
      builder.stream(byteSerde, byteSerde, inTopic)
    }
    val reducedStream: KStream[Null, V] = deserializedStream
      .selectKey[Int]((k, v) => 0)
      .groupByKey()
      .reduce(reducer, reduceTableName)
      .toStream
      .selectKey((k, v) => null)
    writeOut(serialize(reducedStream), outTopic)
    builder
  }
}