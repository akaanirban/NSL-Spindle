package edu.rpi.cs.nsl.spindle.vehicle.kafka.streams

import org.apache.kafka.streams.{KeyValue, StreamsConfig}
import org.apache.kafka.streams.kstream._
import org.slf4j.LoggerFactory
import _root_.edu.rpi.cs.nsl.spindle.vehicle.Configuration
import _root_.edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.{SingleTopicProducerKakfa, TopicLookupService}
import org.apache.kafka.streams.kstream.internals.TimeWindow
import edu.rpi.cs.nsl.spindle.datatypes.operations.ReduceByKeyOperation

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.reflect.runtime.universe._

/**
 * Perform ReduceByKey on stream
 */
class StreamKVReducer[K: TypeTag, V: TypeTag](inTopic: String,
                                              outTopic: String,
                                              reduceFunc: (V, V) => V,
                                              intermediateConfig: StreamsConfig,
                                              startEpochOpt: Option[Long] = None)
    extends TypedStreamExecutor[K, V](startEpochOpt, readableId = s"$inTopic->$outTopic")
      with LocalSelfInitializingExecutor {
  private val logger = LoggerFactory.getLogger(this.getClass)
  logger.debug(s"Creating StreamKVREducer from $inTopic -> $outTopic")
  // Initialize input and output topics
  private val initFuture = initTopics(Set(inTopic, outTopic))
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

  private val windowRetentionMs = 2L * (Configuration.Streams.reduceWindowSizeMs.toLong +
    Configuration.Streams.commitMs.toLong)

  protected val reduceWindows: Windows[TimeWindow] = TimeWindows
    .of(Configuration.Streams.reduceWindowSizeMs)
    .advanceBy(Configuration.Streams.reduceWindowSizeMs)
    .until(windowRetentionMs)
  protected val builder = {
    logger.debug(s"Creating ReduceByKey builder for $inTopic to $outTopic")
    val builder = new KStreamBuilder
    val inStream: ByteStream = builder.stream(byteSerde, byteSerde, inTopic)
    val deserializedStream: KStream[K, V] = deserializeAndFilter(inStream)

    val reducedWindowedStreamName = s"windowed-reduced-${mkRandTopic}"
    val reducedWindowedStream: KStream[Windowed[K], V] = deserializedStream
      .groupByKey
      .reduce(reducer, reduceWindows, reduceTableName)//TODO: wait for watermark then publish
      .toStream()

    val serializedStream = serialize(reducedWindowedStream)
    writeOut(serializedStream, outTopic)

    //TODO: implement batcher in uploader
    /*val batcherSupplier = new StreamBatcherSupplier[K,V](outTopic, clientFactory)
    reducedWindowedStream.process(batcherSupplier)*/

    // Wait for topics to become ready
    Await.ready(initFuture, Duration.Inf)
    builder
  }

  override def handleException(id: String, t: Thread, e: Throwable) {
    logger.error(s"Stream reducer $inTopic -> $outTopic failed")
    super.handleException(id, t, e)
  }
}

object StreamKVReducer {
  def mkReducer[K: TypeTag, V: TypeTag](mapperId: String,
                                        configBuilder: StreamsConfigBuilder,
                                        reduceOperation: ReduceByKeyOperation[V]): StreamKVReducer[K,V] = {
    val inTopic = TopicLookupService.getMapperOutput(mapperId)
    val outTopic = TopicLookupService.getReducerOutput(reduceOperation.uid)
    new StreamKVReducer[K, V](inTopic, outTopic, reduceOperation.f, configBuilder.withId(reduceOperation.uid).build)
  }
}

/*
class StreamBatcherSupplier[K: TypeTag, V: TypeTag](outTopic: String, clientFactory: ClientFactory) extends ProcessorSupplier[Windowed[K],V] {
  private val logger = LoggerFactory.getLogger("StreamBatcherSupplier")
  override def get: Processor[Windowed[K],V] = {
    val producer = clientFactory.mkProducer[K,V](outTopic)
    logger.info(s"Creating stream batcher for topic $outTopic")
    new StreamBatcher[K,V](producer)
  }
}

class StreamBatcher[K: TypeTag, V: TypeTag](producer: SingleTopicProducerKakfa[K,V]) extends Processor[Windowed[K], V] {
 //TODO: see neitszche soln http://stackoverflow.com/questions/39104352/kstream-batch-process-windows
  private val logger = LoggerFactory.getLogger("StreamBatcher")
  private val seenWindows = scala.collection.mutable.Set[Long]() //TODO: use state store
  private val outputBuffer= scala.collection.mutable.Map[Windowed[K], V]()
  private var context: ProcessorContext = _

  override def init(context: ProcessorContext): Unit = {
    this.context = context
    context.schedule(Configuration.Streams.reduceWindowSizeMs)
    logger.info(s"Initialized batcher to run every ${Configuration.Streams.reduceWindowSizeMs} ms")
    //this.seenWindows = mkStore(context)
  }

  override def punctuate(timestamp: Long): Unit = {
    logger.debug(s"Punctuating $timestamp")
    val outputCandidates: Seq[(K, V)] = outputBuffer
      // Get passed windows
      .filterKeys(_.window().end < System.currentTimeMillis())
      .map(kv => (kv._1.window().end(), (kv._1.key, kv._2)))
        .filterNot{case (k,_)=>
            seenWindows.contains(k)
        }
        .map{case(k,v) =>
          seenWindows.add(k)
          logger.debug(s"Updated seen windows: $seenWindows")
          v
        }
        .toSeq
    // Output results
    outputCandidates.foreach{case (k,v) =>
      logger.info(s"Batcher forwarding $k -> $v")
      producer.send(k,v)
    }
    context.commit()
    //TODO: use window expiration config to remove old seenWindows values
  }

  def process(key: Windowed[K], value: V): Unit = {
    logger.debug(s"Processing $key -> $value")
    outputBuffer.put(key, value)
  }

  override def close(): Unit = {
    this.punctuate(System.currentTimeMillis)
    this.producer.flush
    this.producer.close
  }
}*/
