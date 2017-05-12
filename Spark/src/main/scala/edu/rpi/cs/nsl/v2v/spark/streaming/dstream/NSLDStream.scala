package edu.rpi.cs.nsl.v2v.spark.streaming.dstream

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{typeTag, TypeTag}
import org.apache.spark.streaming.dstream.DStream
import edu.rpi.cs.nsl.v2v.spark.streaming.NSLUtils
import edu.rpi.cs.nsl.v2v.spark.streaming.Serialization.{KafkaKey, MiddlewareResults}
import edu.rpi.cs.nsl.spindle.datatypes.operations.OperationIds
import edu.rpi.cs.nsl.spindle.datatypes.operations.MapOperation
import edu.rpi.cs.nsl.spindle.datatypes.operations.ReduceByKeyOperation
import edu.rpi.cs.nsl.spindle.datatypes.operations.Operation
import edu.rpi.cs.nsl.spindle.vehicle.{ReflectionUtils, TypedValue}
import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.ObjectSerializer
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * NSL V2V Spark DStream Wrapper Base Class
 *
 * @see [[https://github.com/apache/spark/blob/master/streaming/src/main/scala
 * /org/apache/spark/streaming/dstream/DStream.scala Spark DStream Base Class]]
 *
 * @see [[https://github.com/apache/spark/blob/v2.0.1/streaming/src/main/scala
 * /org/apache/spark/streaming/dstream/ReceiverInputDStream.scala ReceiverInputDStream]
 *
 * @see [[https://github.com/apache/spark/blob/v2.0.1/streaming/src/main/scala
 * /org/apache/spark/streaming/dstream/InputDStream.scala InputDStream]]
 *
 * @see [[https://github.com/apache/spark/blob/v2.0.1/external/kafka-0-8/src/main
 * /scala/org/apache/spark/streaming/kafka/DirectKafkaInputDStream.scala KafkaInputDStream]]
 *
 */
class NSLDStreamWrapper[T: TypeTag: ClassTag](private[dstream] val generator: NSLUtils.DStreamGenerator,
                                              private[dstream] val opLog: Seq[Operation[_, _]] = Seq())
    extends Serializable {
  /**
   * Get actual DStream
   *
   */
  /*protected[dstream] def toDStream: DStream[T] = {
    val operation = {
      val lastOp = opLog.last
      val lastOpClass = lastOp.getClass.toString
      val reduceClass = ReduceByKeyOperation.getClass.toString.replace("$", "")
      assert(lastOpClass.equals(reduceClass), s"Last operation needs to be a reduce $lastOp")
      lastOp.asInstanceOf[ReduceOperation[T]]
    }
    getMappedStream.reduce(operation.f)
  }*/

  /**
   * Special annotated map function
   *
   * [[https://github.com/apache/spark/blob/master/streaming/src/main/scala/org
   * /apache/spark/streaming/dstream/DStream.scala#L546 DStream Map Function]]
   */
  def map[U: TypeTag: ClassTag](mapFunc: T => U): NSLDStreamWrapper[U] = {
    val operation = MapOperation[T, U](mapFunc)
    new NSLDStreamWrapper(generator, opLog ++ Seq(operation))
  }

  /**
   * Get name of Kafka topic
   */
  private[spark] def getTopic: String = generator.topicName

  /**
   * Overridden Reduce Function
   * [[https://github.com/apache/spark/blob/master/streaming/src/main/scala/org
   * /apache/spark/streaming/dstream/DStream.scala#L596 Original Function]]
    *
    * @todo - add support for full reduce operations in Vehicle-Node program
   */
  /*def reduce(reduceFunc: (T, T) => T, operationId: OperationIds.Value): DStream[T] = {
    val operation = ReduceOperation[T, T](reduceFunc, operationId)
    new NSLDStreamWrapper(generator, opLog ++ Seq(operation)).toDStream
  }*/

  private[dstream] def getStream[K: TypeTag, V: TypeTag]: DStream[(Array[Byte],Array[Byte])] = {
    generator.mkStream(opLog)
      .filter(_ != null)
      .map(record => (record.key(), record.value()))
  }
}

object NSLDStreamWrapper {
  implicit def toPairFunctions[K: TypeTag: ClassTag, V: TypeTag: ClassTag](stream: NSLDStreamWrapper[(K, V)]): PairFunctions[K, V] = {
    new PairFunctions[K, V](stream)
  }
}

/**
  * Deserializiation and reduction utilities with clean closure
  */
object DeserializationUtils {
  def isCanary(bytes: Array[Byte]): Boolean = {
    ObjectSerializer.deserialize[TypedValue[_]](bytes).isCanary
  }
  def isType[T: TypeTag](bytes: Array[Byte]): Boolean = {
    val taggedElem: TypedValue[T] = ObjectSerializer.deserialize[TypedValue[T]](bytes)
    val bytesType = taggedElem.getType
    println(s"TODO: compare types: ${typeTag[T].tpe} ?= $bytesType aka ${taggedElem.getClassString} -> ${bytesType =:= typeTag[T].tpe}")
    //TODO: actually validate type
    true
  }
  def reduceByKeyOnStream[K: TypeTag: ClassTag, V: TypeTag: ClassTag](reduceFunc: (V, V) => V, rawStream: DStream[(Array[Byte], Array[Byte])]) = {
    val filteredStream = rawStream.filter{case (k, _) => isCanary(k) == false}
      // Make sure data types are correct
      .filter{case (k,v) =>
        println(s"Checking type on ($k,$v)")
        isType[K](k) && isType[V](v)
      }
    val deserializedStream = filteredStream.map{case(k,v) =>
      (ObjectSerializer.deserialize[TypedValue[K]](k).value, ObjectSerializer.deserialize[TypedValue[V]](v).value)
    }
    deserializedStream.reduceByKey(reduceFunc)
  }
}

/**
 * Implicit pair operations based off Spark PairDStreamFunctions
 *
 * @see [[https://github.com/apache/spark/blob/master/streaming/src/main/scala
 * /org/apache/spark/streaming/dstream/PairDStreamFunctions.scala Spark PairDStream Functions]]
 */
class PairFunctions[K: TypeTag: ClassTag, V: TypeTag: ClassTag](streamWrapper: NSLDStreamWrapper[(K, V)]) extends Serializable {
  def reduceByKey(reduceFunc: (V, V) => V, operationId: OperationIds.Value = OperationIds.sum): DStream[(K, V)] = {
    val operation = ReduceByKeyOperation[V](reduceFunc, operationId)
    new NSLDStreamWrapper[(K, V)](streamWrapper.generator, streamWrapper.opLog ++ Seq(operation)).toKVDStream
  }

  protected def toKVDStream: DStream[(K, V)] = {
    val operation = {
      val lastOp = streamWrapper.opLog.last
      val lastOpClass = lastOp.getClass.toString
      val reduceClass = ReduceByKeyOperation.getClass.toString.replace("$", "")
      assert(lastOpClass.equals(reduceClass), s"Last operation needs to be a reduce $lastOpClass != $reduceClass")
      lastOp.asInstanceOf[ReduceByKeyOperation[V]]
    }
    DeserializationUtils.reduceByKeyOnStream(operation.f, streamWrapper.getStream[K,V])
  }
}
