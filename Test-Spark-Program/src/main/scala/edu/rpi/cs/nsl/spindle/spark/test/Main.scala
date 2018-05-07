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

    var ip = "MIDDLEWARE-KAFKA"
    if (args.length != 0) {
      ip = args(0)
    }

    println(s"using middleware IP: $ip")
    val firstIp = s"$ip:2181"
    val secondIp = s"$ip:9092"

    val stream = NSLUtils.createVStream(ssc, NSLUtils.StreamConfig(firstIp, secondIp, TOPIC), new MockQueryUidGenerator)
      .map(v => (null, (v.mph, 1.toLong)))
      .reduceByKey{case (a,b) => (a._1 + b._1, a._2 + b._2)}
      .print()

    ssc.start()
    ssc.awaitTermination()
  }
}
