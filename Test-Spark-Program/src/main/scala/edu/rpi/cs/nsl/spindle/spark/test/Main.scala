package edu.rpi.cs.nsl.spindle.spark.test

import edu.rpi.cs.nsl.spindle.QueryUidGenerator
import edu.rpi.cs.nsl.spindle.datatypes.Vehicle
import edu.rpi.cs.nsl.v2v.spark.streaming.NSLUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
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



class MockQueryUidGenerator extends QueryUidGenerator {
  override def getQueryUid: String = "globalSpeedAvg"
}

/**
  * Created by wrkronmiller on 5/3/17.
  */
object Main {
  type ByteArray = Array[Byte]
  def isCanary(bytes: ByteArray): Boolean = {
    ObjectSerializer.deserialize[TypedValue[_]](bytes).isCanary
  }

  def main(args: Array[String]): Unit = {
    val TOPIC = "spindle-vehicle-middleware-input"
    val sc = new SparkConf().setAppName("SparkSpindleTest").setMaster("local[*]")
    val ssc = new StreamingContext(sc, Seconds(15))
    //val stream = NSLUtils.createVStream(ssc, NSLUtils.StreamConfig("hadrian.kronmiller.net:2181", "hadrian.kronmiller.net:9092", TOPIC), new MockQueryUidGenerator)
    //  .map(v => (null, (v.mph, 1.toLong)))
    //  .reduceByKey{case (a,b) => (a._1 + b._1, a._2 + b._2)}
    //  .print()
		//val stream = NSLUtils.createVStream(ssc, NSLUtils.StreamConfig("127.0.0.1:2181", "127.0.0.1:9092", TOPIC), new MockQueryUidGenerator)
    //  .map(v => (null, 2.toDouble))

//    val stream = NSLUtils.createVStream(ssc, NSLUtils.StreamConfig("127.0.0.1:2181", "127.0.0.1:9092", TOPIC), new MockQueryUidGenerator)
//      .map(v => (null, (v.mph, 1.toLong)))
//      .reduceByKey{case (a,b) => (a._1 + b._1, a._2 + b._2)}
//      .print()
//		val stream = NSLUtils.createVStream(ssc, NSLUtils.StreamConfig("127.0.0.1:2181", "127.0.0.1:9092", TOPIC), new MockQueryUidGenerator)
//      .map(v => (null, 1.toDouble))
//      .reduceByKey{case (a,b) => (a + b)}
//      .print()


		val (rawStream, name) = NSLUtils.createVStream(ssc, NSLUtils.StreamConfig("127.0.0.1:2181", "127.0.0.1:9092", TOPIC), new MockQueryUidGenerator).getStream[String, (Double, Long)]
    val queryId = "globalSpeedAvg"
    print("name is:")
    print(name)

    val filteredStream = rawStream.filter{case (k, _) => isCanary(k) == false}
      // Make sure data types are correct
      .filter{case (kSer,vSer) =>
      ObjectSerializer.checkQueryIdMatch(queryId, kSer,vSer)
    }
    val deserializedStream = filteredStream.map{case(serKey,serVal) =>
      val keyTyped = ObjectSerializer.deserialize[TypedValue[String]](serKey)
      val valTyped = ObjectSerializer.deserialize[TypedValue[(Double,Double)]](serVal)
      (keyTyped.value, valTyped.value)
    }

    deserializedStream.print()


    ssc.start()
    ssc.awaitTermination()
  }
}
