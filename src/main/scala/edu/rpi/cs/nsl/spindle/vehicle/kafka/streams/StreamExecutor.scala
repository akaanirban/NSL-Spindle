package edu.rpi.cs.nsl.spindle.vehicle.kafka.streams


import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.{KStream, Windowed}
import org.apache.kafka.streams.processor.TopologyBuilder
import org.slf4j.LoggerFactory
import _root_.edu.rpi.cs.nsl.spindle.vehicle.TypedValue
import _root_.edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.KafkaSerde
import _root_.edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.ObjectSerializer

import scala.reflect.runtime.universe.TypeTag
import java.lang.Thread.UncaughtExceptionHandler
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import akka.pattern.AskTimeoutException
import org.apache.kafka.clients.consumer.CommitFailedException

import scala.concurrent._
import scala.concurrent.duration._


/**
 * Executor that runs a Kafka Streams program
  *
  * @todo - filter messages that arrive before specified time
 */
abstract class StreamExecutor(startEpochOpt: Option[Long] = None) {
  type ByteArray = Array[Byte]
  protected type ByteStream = KStream[ByteArray, ByteArray]
  private val logger = LoggerFactory.getLogger("Stream executor")
  protected val builder: TopologyBuilder
  protected val config: StreamsConfig
  private val MAX_WAIT_READY_ITERATIONS = 20

  protected val byteSerde = Serdes.ByteArray

  private var stream: KafkaStreams = _

  private val startTime: Long = System.currentTimeMillis()


  protected def deserializeAndFilter[K: TypeTag, V: TypeTag](inStream: ByteStream): KStream[K, V] = {
    val typedStream: KStream[TypedValue[K], TypedValue[V]] = inStream
      .filterNot{(k,_) =>
        val msg = ObjectSerializer.deserialize[TypedValue[Any]](k)
        System.err.println(s"Checking if canary: $msg")
        msg.isCanary
      }
      .map { (k, v) =>
        val msg = new KeyValue(ObjectSerializer.deserialize[TypedValue[K]](k), ObjectSerializer.deserialize[TypedValue[V]](v))
        System.err.println(s"Deserialized message $msg")
        msg
      }

    val filteredStream = startEpochOpt match {
      case None => typedStream
      case Some(startEpoch) => {
        typedStream.filter{(k,v) =>
          logger.debug(s"Filtering start times before $startEpoch")
          System.err.println(s"Filtering start times before $startEpoch: ($k, $v)")
          k.creationEpoch >= startEpoch && v.creationEpoch >= startEpoch
        }
      }
    }
    filteredStream.map{(k,v) =>
      new KeyValue[K,V](k.value, v.value)
    }
  }

  protected def serialize[K: TypeTag, V: TypeTag](objStream: KStream[K, V]): ByteStream = {
    objStream.map { (k, v) =>
      new KeyValue(ObjectSerializer.serialize(TypedValue[K](k)), ObjectSerializer.serialize(TypedValue[V](v)))
    }
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
      case e: Any => {
        logger.error(s"Unknown exception in stream executor. Killing process")
        e.printStackTrace(System.err)
        System.exit(1)
      }
    }
  }

  private lazy val id: String = config.getString(StreamsConfig.APPLICATION_ID_CONFIG)

  def run {

    logger.debug(s"Building stream $id")
    stream = new KafkaStreams(builder, config)
    logger.info(s"Starting stream $id")
    stream.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
      def uncaughtException(t: Thread, e: Throwable) {
        handleException(id, t, e)
      }
    })

    stream.start()
    logger.info(s"Stream $id has started")
  }

  def stopStream: Future[Any] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    logger.info(s"Stream stopping: $id")
    Future {
      blocking {
        logger.info(s"Calling close on stream $id")
        stream.close()
        logger.info(s"Closed stream $id")
        true
      }
    }
  }
}

abstract class TypedStreamExecutor[K: TypeTag, V: TypeTag](startEpochOpt: Option[Long] = None)
  extends StreamExecutor(startEpochOpt) {
  protected val keySerde = new KafkaSerde[TypedValue[K]]
  protected val valueSerde = new KafkaSerde[TypedValue[V]]
}
