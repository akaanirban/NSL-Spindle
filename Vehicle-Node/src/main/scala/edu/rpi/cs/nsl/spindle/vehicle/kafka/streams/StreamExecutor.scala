package edu.rpi.cs.nsl.spindle.vehicle.kafka.streams


import org.apache.kafka.common.serialization.{ByteArraySerializer, Serdes, Serializer}
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.processor.TopologyBuilder
import org.slf4j.LoggerFactory
import _root_.edu.rpi.cs.nsl.spindle.vehicle.{ReflectionUtils, TypedValue}
import _root_.edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.KafkaSerde
import _root_.edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.ObjectSerializer

import scala.reflect.runtime.universe.TypeTag
import java.lang.Thread.UncaughtExceptionHandler
import java.util.concurrent.TimeUnit

import edu.rpi.cs.nsl.spindle.vehicle.connections.KafkaConnection
import kafka.producer.Producer
import org.apache.kafka.clients.consumer.CommitFailedException
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier

import scala.concurrent._


/**
 * Executor that runs a Kafka Streams program
  *
  * @todo - filter messages that arrive before specified time
 */
abstract class StreamExecutor(startEpochOpt: Option[Long] = None, readableId: String = "") {
  type ByteArray = Array[Byte]
  protected type ByteStream = KStream[ByteArray, ByteArray]
  private val logger = LoggerFactory.getLogger("Stream executor")
  protected val builder: TopologyBuilder
  protected val config: StreamsConfig
  private val MAX_WAIT_READY_ITERATIONS = 20

  protected val byteSerde = Serdes.ByteArray

  private var stream: KafkaStreams = _

  private val startTime: Long = System.currentTimeMillis()

  private def getAnyTyped(msgBytes: Array[Byte]): TypedValue[Any] = {
    ObjectSerializer.deserialize[TypedValue[Any]](msgBytes)
  }
  protected def deserializeAndFilter[K: TypeTag, V: TypeTag](inStream: ByteStream): KStream[K, V] = {
    val (kTypeStr, vTypeStr): (String, String) = {
      (ReflectionUtils.getTypeString[K], ReflectionUtils.getTypeString[V])
    }
    val typedStream: KStream[TypedValue[K], TypedValue[V]] = inStream
        .filterNot{(k,v) =>
          k == null || v == null
        }
      .filterNot{(k,_) =>
        val msg = getAnyTyped(k)
        logger.trace(s"Checking if canary: $msg")
        msg.isCanary
      }
      .filter{(k,v)=>
        val msgKTypeStr = getAnyTyped(k).getTypeString
        val msgVTypeStr = getAnyTyped(v).getTypeString
        val keyMatch = (msgKTypeStr == kTypeStr)
        logger.trace(s"Checking if keymatch $msgKTypeStr ?= $kTypeStr: $keyMatch")
        val valMatch = (msgVTypeStr == vTypeStr)
        logger.trace(s"Checking of valmatch $msgVTypeStr ?= $vTypeStr: $valMatch")
        val typeMatch = keyMatch && valMatch
        logger.debug(s"Checking typematch for $inStream: $typeMatch, $msgKTypeStr -> $msgVTypeStr")
        typeMatch
      }
      .map { (k, v) =>
        val msg = new KeyValue(ObjectSerializer.deserialize[TypedValue[K]](k), ObjectSerializer.deserialize[TypedValue[V]](v))
        logger.trace(s"Deserialized message $msg")
        msg
      }

    val filteredStream = startEpochOpt match {
      case None => typedStream
      case Some(startEpoch) => {
        typedStream.filter{(k,v) =>
          logger.trace(s"Filtering start times before $startEpoch: ($k, $v)")
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

  private def restartStream: Unit = {
    logger.error(s"Closing stream $id")
    stream.close
    logger.info(s"Restarting stream $id")
    this.run
    logger.info(s"Stream $id restarted")
  }

  protected def handleException(id: String, t: Thread, e: Throwable) {
    e match {
      case cfe: CommitFailedException => {
        logger.warn(s"Commit failed $cfe")
        restartStream
      }
      case _: java.lang.InterruptedException => {
        logger.warn(s"Stream interrupted: $id")
      }
      case e: Any => {
        logger.error(s"Unknown exception in stream executor. Killing process")
        e.printStackTrace(System.err)
        System.err.println(s"Exception cause ${e.getCause}")
        throw e
      }
    }
  }

  private lazy val id: String = config.getString(StreamsConfig.APPLICATION_ID_CONFIG)

  def run {
    //logger.debug(s"Building stream $id")
    System.err.println(s"Building stream $id $builder $config")
    //KafkaProducer.java Line 336
    //stream = new KafkaStreams(builder, config, new KafkaClientSupplier) //TODO: debug this
    System.err.println(s"Starting stream $id")
    /*stream.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
      def uncaughtException(t: Thread, e: Throwable) {
        handleException(id, t, e)
      }
    })

    stream.start()*/ //TODO: restore this
    println(s"Stream $id has started")
  }

  def stopStream: Future[Any] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    logger.info(s"Stream stopping: $id")
    Future {
      blocking {
        //logger.info(s"Calling close on stream $id")
        System.out.println(s"Calling close on stream $id")
        stream.close()
        stream.cleanUp()
        //logger.info(s"Closed stream $id")
        System.out.println(s"Closed stream $id: ${stream.toString}")
        true
      }
    }
  }
}

/**
  * Client supplier for kafka streams
  */
class KafkaClientSupplier extends DefaultKafkaClientSupplier {
  import scala.collection.JavaConversions._
  println("Using debug kafka client suppliser")
  private class DebugProducer[K,V](configs: java.util.Map[String, AnyRef], keySerializer: Serializer[K], valueSerializer: Serializer[V])
    extends KafkaProducer[K,V](configs: java.util.Map[String, AnyRef], keySerializer: Serializer[K], valueSerializer: Serializer[V]) {
    override def close(): Unit = {
      println(s"Closing producer ${configs.toList}")
      super.close()
      println(s"Closed producer ${configs.toList}")
    }

    override def close(timeout: Long, timeUnit: TimeUnit): Unit = {
      println(s"Closing producer ${configs.toList} with timeout $timeout")
      super.close(timeout, timeUnit)
      println(s"Closed producer ${configs.toList}")
    }
    //TODO
  }
  override def getProducer(config: java.util.Map[String, AnyRef]): KafkaProducer[Array[Byte], Array[Byte]] = {
    println(s"Creating producer with config ${config.toList}")
    new DebugProducer[Array[Byte], Array[Byte]](config, new ByteArraySerializer(), new ByteArraySerializer())
  }
}

abstract class TypedStreamExecutor[K: TypeTag, V: TypeTag](startEpochOpt: Option[Long] = None, readableId: String = "")
  extends StreamExecutor(startEpochOpt, readableId) {
  protected val keySerde = new KafkaSerde[TypedValue[K]]
  protected val valueSerde = new KafkaSerde[TypedValue[V]]
}

/**
  * Local Kafka Streams Executor that initializes own topics
  */
trait LocalSelfInitializingExecutor {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val kafkaConnection = KafkaConnection.getLocal
  protected def initTopics(topics: Set[String])(implicit ec: ExecutionContext): Future[Unit] ={
    val admin = kafkaConnection.getAdmin
    val canaryProducer = kafkaConnection.getProducer[Any, Any]

    val initFutures: Seq[Future[_]] = topics.toSeq.map{topic =>
      println(s"Creating topic $topic")
      admin.mkTopic(topic)
        .map { _ =>
          println(s"Created topic $topic")
          topic
        }
        .flatMap(canaryProducer.sendKafka(_, None, None, true))
    }
    println(s"Initializing topics $topics")
    Future
      .sequence(initFutures)
      .map(_ => println(s"Initialized topics $topics"))
      .map(_ => {
        println("Closing canary producer")
        canaryProducer.close
        println("Canary producer closed")
        println("Closing session admin")
        admin.close
        println("Closed session admin")
      })
  }
}