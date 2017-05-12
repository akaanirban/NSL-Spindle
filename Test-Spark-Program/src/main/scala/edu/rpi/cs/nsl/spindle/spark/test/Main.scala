package edu.rpi.cs.nsl.spindle.spark.test

import edu.rpi.cs.nsl.v2v.spark.streaming.NSLUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * Created by wrkronmiller on 5/3/17.
  */
object Main {
  def main(args: Array[String]): Unit = {
    val TOPIC = "spindle-vehicle-reducer-sumSpeedAndCount"
    val sc = new SparkConf().setAppName("SparkSpindleTest").setMaster("local[*]")
    val ssc = new StreamingContext(sc, Seconds(1))
    val stream = NSLUtils.createVStream(ssc, NSLUtils.StreamConfig("localhost:2182", "localhost:9093", TOPIC))
      .map(v => (null, (v.mph, 1)))
      .reduceByKey{case (a,b) => (a._1 + b._1, a._2 + b._2)}
      .print()
    ssc.start()
    ssc.awaitTermination()
  }
}
