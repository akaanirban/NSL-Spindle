package edu.rpi.cs.nsl.spindle.vehicle.streams

import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream._
import org.slf4j.LoggerFactory
import edu.rpi.cs.nsl.spindle.vehicle.kafka_utils.ObjectSerializer

/**
 * Executor that runs a Kafka Streams program
 */
abstract class StreamExecutor extends Runnable {
  type ByteArray = Array[Byte]
  private val logger = LoggerFactory.getLogger(this.getClass)
  protected val builder: KStreamBuilder
  protected val config: StreamsConfig

  protected val byteSerde = Serdes.ByteArray

  def run {
    val id = config.getString(StreamsConfig.APPLICATION_ID_CONFIG)
    logger.debug(s"Building stream $id")
    val stream = new KafkaStreams(builder, config)
    logger.info(s"Starting stream $id")
    stream.start
    logger.error(s"Stream $id has stopped")
  }
}