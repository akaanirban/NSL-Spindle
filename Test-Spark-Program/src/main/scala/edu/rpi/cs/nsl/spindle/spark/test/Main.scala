package edu.rpi.cs.nsl.spindle.spark.test

import edu.rpi.cs.nsl.spindle.QueryUidGenerator
import edu.rpi.cs.nsl.v2v.spark.streaming.NSLUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}


class MockQueryUidGenerator extends QueryUidGenerator {
  override def getQueryUid: String = "globalSpeedAvg"
}

/**
  * Created by wrkronmiller on 5/3/17.
  */
object Main {
  def main(args: Array[String]): Unit = {
    val TOPIC = "spindle-vehicle-middleware-input"
    val sc = new SparkConf().setAppName("SparkSpindleTest").setMaster("local[*]")
    val ssc = new StreamingContext(sc, Seconds(15))
    //val stream = NSLUtils.createVStream(ssc, NSLUtils.StreamConfig("hadrian.kronmiller.net:2181", "hadrian.kronmiller.net:9092", TOPIC), new MockQueryUidGenerator)
    //  .map(v => (null, (v.mph, 1.toLong)))
    //  .reduceByKey{case (a,b) => (a._1 + b._1, a._2 + b._2)}
    //  .print()
		//val stream = NSLUtils.createVStream(ssc, NSLUtils.StreamConfig("127.0.0.1:2181", "127.0.0.1:9092", TOPIC), new MockQueryUidGenerator)
		val stream = NSLUtils.createVStream(ssc, NSLUtils.StreamConfig("127.0.0.1:2181", "127.0.0.1:9092", TOPIC), new MockQueryUidGenerator)
			.map(v => (v))
			//.map(foo)
			//.reduceByKey{bar}
      .reduceByKey{case (a,b) => (a, b)}
			.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
