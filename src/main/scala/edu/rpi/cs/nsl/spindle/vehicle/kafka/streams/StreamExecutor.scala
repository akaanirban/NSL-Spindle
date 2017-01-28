package edu.rpi.cs.nsl.spindle.vehicle.kafka.streams

import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream._
import org.slf4j.LoggerFactory

import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.ObjectSerializer;
import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.KafkaSerde
import org.apache.kafka.streams.processor.TopologyBuilder

/**
 * Executor that runs a Kafka Streams program
 */
abstract class StreamExecutor extends Thread {
  type ByteArray = Array[Byte]
  protected type ByteStream = KStream[ByteArray, ByteArray]
  private val logger = LoggerFactory.getLogger(this.getClass)
  protected val builder: TopologyBuilder
  protected val config: StreamsConfig

  protected val byteSerde = Serdes.ByteArray

  private var stream: KafkaStreams = _

  protected def deserialize[K, V](inStream: ByteStream): KStream[K, V] = {
    inStream.map { (k, v) => new KeyValue(ObjectSerializer.deserialize(k), ObjectSerializer.deserialize(v)) }
  }

  protected def serialize[K, V](objStream: KStream[K, V]): ByteStream = {
    objStream.map { (k, v) => new KeyValue(ObjectSerializer.serialize(k), ObjectSerializer.serialize(v)) }
  }

  protected def writeOut(outStream: ByteStream, outTopic: String) = {
    outStream.to(byteSerde, byteSerde, outTopic)
  }

  override def run {
    val id = config.getString(StreamsConfig.APPLICATION_ID_CONFIG)
    try {
      logger.debug(s"Building stream $id")
      stream = new KafkaStreams(builder, config)
      logger.info(s"Starting stream $id")
      stream.start
      logger.error(s"Stream $id has stopped")
    } catch {
      case e: Exception => {
        logger.error(s"Failure in stream executor $id: ${e.getMessage}")
        throw e
      }
    }
  }

  def stopStream {
    logger.info(s"Stream stopping: ${config.getString(StreamsConfig.APPLICATION_ID_CONFIG)}")
    stream.close
  }
}

abstract class TypedStreamExecutor[K >: Null, V >: Null] extends StreamExecutor {
  protected val keySerde = new KafkaSerde[K]
  protected val valueSerde = new KafkaSerde[V]
}