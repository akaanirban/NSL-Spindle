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
import org.apache.kafka.streams.kstream.KeyValueMapper
import scala.reflect.runtime.universe._

/**
 * Perform pure reduce on stream
 *
 * @note - strips keys
 *
 * @todo - remove nulls (this will require changes to the Kafka consumer as well)
 *
 * @see [[https://github.com/confluentinc/examples/blob/10eafe95a972bf18b9f681129404044ef36ee8cc
 * /kafka-streams/src/main/java/io/confluent/examples/streams/SumLambdaExample.java#L116 Confluent Reduce Example]]
 */
class StreamReducer[K: TypeTag, V: TypeTag](inTopic: String, outTopic: String, reduceFunc: (V, V) => V, intermediateConfig: StreamsConfig)
    extends StreamKVReducer[K, V](inTopic: String, outTopic: String, reduceFunc: (V, V) => V, intermediateConfig: StreamsConfig) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  logger.info(s"Creating reducer from $inTopic -> $outTopic")

  override protected val builder = {
    val builder = new KStreamBuilder
    val deserializedStream: KStream[K, V] = deserialize {
      builder.stream(byteSerde, byteSerde, inTopic)
    }
    //scalastyle:off null
    val reducedStream: KStream[Null, V] = deserializedStream
      .selectKey[Int]((k, v) => 0)
      .groupByKey()
      .reduce(reducer, reduceTableName)
      .toStream
      .selectKey((k, v) => null)
    //scalastyle:on null
    writeOut(serialize(reducedStream), outTopic)
    builder
  }
}
