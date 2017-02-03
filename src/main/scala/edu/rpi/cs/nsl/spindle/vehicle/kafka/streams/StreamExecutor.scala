package edu.rpi.cs.nsl.spindle.vehicle.kafka.streams

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.processor.TopologyBuilder
import org.slf4j.LoggerFactory

import _root_.edu.rpi.cs.nsl.spindle.vehicle.TypedValue
import _root_.edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.KafkaSerde
import _root_.edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.ObjectSerializer
import scala.reflect.runtime.universe.TypeTag
import java.lang.Thread.UncaughtExceptionHandler
import org.apache.kafka.clients.consumer.CommitFailedException

/**
 * Executor that runs a Kafka Streams program
 */
abstract class StreamExecutor {
  protected val ready = new AtomicBoolean(false)
  type ByteArray = Array[Byte]
  protected type ByteStream = KStream[ByteArray, ByteArray]
  private val logger = LoggerFactory.getLogger(this.getClass)
  protected val builder: TopologyBuilder
  protected val config: StreamsConfig
  private val MAX_WAIT_READY_ITERATIONS = 20

  protected val byteSerde = Serdes.ByteArray

  private var stream: KafkaStreams = _

  protected def deserialize[K: TypeTag, V: TypeTag](inStream: ByteStream): KStream[K, V] = {
    inStream.map { (k, v) =>
      new KeyValue(ObjectSerializer.deserialize[TypedValue[K]](k).value, ObjectSerializer.deserialize[TypedValue[V]](v).value)
    }
  }

  protected def serialize[K: TypeTag, V: TypeTag](objStream: KStream[K, V]): ByteStream = {
    objStream.map { (k, v) => new KeyValue(ObjectSerializer.serialize(TypedValue[K](k)), ObjectSerializer.serialize(TypedValue[V](v))) }
  }

  protected def writeOut(outStream: ByteStream, outTopic: String) = {
    outStream.to(byteSerde, byteSerde, outTopic)
  }

  protected def handleException(id: String, t: Thread, e: Throwable) {
    logger.error(s"Stream $id encountered exception $e")
    e match {
      case cfe: CommitFailedException => {
        logger.error(s"Closing stream $id")
        stream.close
        logger.info(s"Restarting stream $id")
        this.run
        logger.info(s"Stream $id restarted")
      }
      case _ => System.exit(1)
    }
  }

  def run {
    assert(this.ready.get == false)
    val id = config.getString(StreamsConfig.APPLICATION_ID_CONFIG)
    logger.debug(s"Building stream $id")
    stream = new KafkaStreams(builder, config)
    logger.info(s"Starting stream $id")
    stream.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
      def uncaughtException(t: Thread, e: Throwable) {
        handleException(id, t, e)
      }
    })

    stream.start

    logger.error(s"Stream $id has started")
    this.ready.set(true)
  }

  def stopStream {
    def waitReady(iterations: Int = 0) {
      if (this.ready.get == false) {
        if (iterations > MAX_WAIT_READY_ITERATIONS) {
          throw new RuntimeException(s"Cannot close executor because executor never came online")
        }
        logger.debug(s"Stream executor waiting to become ready before stopping")
        Thread.sleep(100)
        waitReady(iterations + 1)
      }
    }
    logger.info(s"Stream stopping: ${config.getString(StreamsConfig.APPLICATION_ID_CONFIG)}")
    stream.close
  }
}

abstract class TypedStreamExecutor[K: TypeTag, V: TypeTag] extends StreamExecutor {
  protected val keySerde = new KafkaSerde[TypedValue[K]]
  protected val valueSerde = new KafkaSerde[TypedValue[V]]
}
