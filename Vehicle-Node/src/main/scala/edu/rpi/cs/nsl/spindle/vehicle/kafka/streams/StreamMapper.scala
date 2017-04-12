package edu.rpi.cs.nsl.spindle.vehicle.kafka.streams

import edu.rpi.cs.nsl.spindle.datatypes.operations.MapOperation
import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.TopicLookupService
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.slf4j.LoggerFactory
import org.apache.kafka.streams.KeyValue

import scala.reflect.runtime.universe._

/**
 * Kafka streams mapper executor
 *
 * @see [[https://kafka.apache.org/0100/javadoc/org/apache/kafka/streams/kstream/KStream.html
 * #map(org.apache.kafka.streams.kstream.KeyValueMapper) Mapper Documentation]]
 */
class StreamMapper[K: TypeTag, V: TypeTag, K1: TypeTag, V1: TypeTag](inTopic: String,
                                 outTopic: String,
                                 mapFunc: ((K, V)) => (K1, V1),
                                 filterFunc: (K,V) => Boolean,
                                 protected val config: StreamsConfig,
                                 startEpochOpt: Option[Long] = None)
    extends StreamExecutor(startEpochOpt) {
  private val logger = LoggerFactory.getLogger(s"StreamMapper $inTopic -> $outTopic")
  logger.debug(s"Created StreamMapper $inTopic -> $outTopic")
  protected val builder = {
    logger.debug(s"Configuring mapper builder for $inTopic to $outTopic")
    val builder = new KStreamBuilder
    val inStream: ByteStream = builder.stream(inTopic)
    val deserializedStream: KStream[K, V] = deserializeAndFilter(inStream)
    val mappedStream: KStream[K1, V1] = deserializedStream
    .filter{(k,v)=>
        filterFunc(k,v)
    }
    .map { (k, v) =>
      val (k1, v1) = mapFunc((k, v))
      //logger.debug(s"Mapping $k, $v -> $k1, $v1")
      logger.trace(s"Mapping $k, $v -> $k1, $v1")
      new KeyValue(k1, v1)
    }
    val serializedStream: ByteStream = serialize(mappedStream)
    writeOut(serializedStream, outTopic)
    builder
  }
}

object StreamMapper {
  def mkMapper[K: TypeTag, V: TypeTag, K1: TypeTag, V1: TypeTag](configBuilder: StreamsConfigBuilder, mapOperation: MapOperation[(K, V), (K1,V1)]): StreamMapper[K,V,K1,V1] = {
    val inTopic = TopicLookupService.getVehicleStatus
    val outTopic = TopicLookupService.getMapperOutput(mapOperation.uid)
    def nopFilter(k:K, v:V) = true //TODO: add filter() to spark streaming
    new StreamMapper[K,V,K1,V1](inTopic, outTopic, mapOperation.f, filterFunc=nopFilter, config=configBuilder.withId(mapOperation.uid).build)
  }
}