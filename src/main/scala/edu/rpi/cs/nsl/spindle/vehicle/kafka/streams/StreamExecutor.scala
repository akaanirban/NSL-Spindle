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
abstract class StreamExecutor {
  type ByteArray = Array[Byte]
  protected type ByteStream = KStream[ByteArray, ByteArray]
  private val logger = LoggerFactory.getLogger("Stream executor")
  protected val builder: TopologyBuilder
  protected val config: StreamsConfig
  private val MAX_WAIT_READY_ITERATIONS = 20
  private val started: AtomicBoolean = new AtomicBoolean(false)

  protected val byteSerde = Serdes.ByteArray

  private var stream: KafkaStreams = _

  protected def deserialize[K: TypeTag, V: TypeTag](inStream: ByteStream): KStream[K, V] = {
    inStream.map { (k, v) =>
      new KeyValue(ObjectSerializer.deserialize[TypedValue[K]](k).value, ObjectSerializer.deserialize[TypedValue[V]](v).value)
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
    this.started.set(true)

    logger.info(s"Stream $id has started")
    System.err.println(s"Stream $id has started") //TODO: remove
  }

  def stopStream {
    import scala.concurrent.ExecutionContext.Implicits.global
    val STOP_WAIT_TIME: FiniteDuration = (30 seconds)
    logger.info(s"Stream stopping: $id")
    if(this.started.getAndSet(false) == false) {
      val message = s"Attempting to stop stream that is not started: $id"
      System.err.println(message)
      logger.error(message)
      throw new RuntimeException(message)
    }
    try {
      val closeFuture: Future[Boolean] = Future {
        blocking {
          logger.info(s"Calling close on stream $id")
          stream.close()
          true
        }
      }
      val timeoutFuture = Future[Boolean] {
        blocking {
          Thread.sleep(STOP_WAIT_TIME.toMillis)
          false
        }
      }
      val closed: Boolean = Await.result(Future.firstCompletedOf(Seq(closeFuture, timeoutFuture)), Duration.Inf)
      if(closed) {
        logger.info(s"Cleaning up closed stream $id")
        stream.cleanUp()
      } else {
        logger.error(s"Attempt to close stream timed out $id")
      }
    } catch {
      case badState: IllegalStateException => {
        logger.error(s"Failed to stop stream: $id due to state error $badState")
      }
    }
    logger.info(s"Stream stopped: $id")
  }
}

abstract class TypedStreamExecutor[K: TypeTag, V: TypeTag] extends StreamExecutor {
  protected val keySerde = new KafkaSerde[TypedValue[K]]
  protected val valueSerde = new KafkaSerde[TypedValue[V]]
}
