package edu.rpi.cs.nsl.v2v.spark

import org.apache.spark.streaming.StreamingContext

import edu.rpi.cs.nsl.v2v.spark.streaming.NSLUtils
import edu.rpi.cs.nsl.spindle.datatypes.operations.OperationIds
import edu.rpi.cs.nsl.spindle.datatypes.Vehicle
import scala.util.Random
import edu.rpi.cs.nsl.spindle.datatypes.VehicleColors
import java.util.Properties
import org.apache.kafka.clients.producer.KafkaProducer
import kafka.producer.Producer

import edu.rpi.cs.nsl.v2v.spark.streaming.Serialization.{ KafkaKey, MiddlewareResults }
import org.apache.kafka.clients.producer.ProducerRecord

/**
 * Spark Streaming unit tests
 *
 * @see [[http://mkuthan.github.io/blog/2015/03/01/spark-unit-testing/ Spark and Spark Streaming Unit Testing]] for source of inspiration
 */
class StreamingSpec extends NSLSpec with StreamingFixtures {
  private var ssc: StreamingContext = _
  before {
    ssc = getSSC
    this.startContainers
  }

  /**
   * Basic sanity check
   */
  ignore should "create an RDD from a list" in {
    Given("A range of numbers")
    val collection = (0 to 1E2.toInt).toList

    When("The collection is parallelized")
    val rdd = ssc.sparkContext.parallelize(collection)

    Then("The RDD count should equal the collection size")
    assert(rdd.count === collection.length)
  }

  trait InitializedStream {
    val zkQuorum = getZkConnection
    val kafkaBrokers = getKafkaConnection
    val streamConfig = NSLUtils.StreamConfig(zkQuorum, kafkaBrokers)
    lazy val vStream = NSLUtils.createVStream(ssc, streamConfig)
    lazy val topic = vStream.getTopic
    def publishData(f: (Vehicle) => Any): Thread = {
      val thread = new Thread {
        def mkData: Vehicle = {
          import VehicleColors._
          val id = Random.nextLong
          val lat = Random.nextDouble
          val lon = Random.nextDouble
          val mph = Random.nextDouble * 80 // Somewhat sane speed
          val colors = Seq(red, orange, yellow, green, blue, purple, gray, white, black)
          val color = Random.shuffle(colors).last
          Vehicle(id, lat, lon, mph, color)
        }
        override def run {
          val producer = {
            val props = new Properties()
            props.put("bootstrap.servers", kafkaBrokers)
            props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
            props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
            new KafkaProducer[Array[Byte], Array[Byte]](props)
          }

          def sendMessage {
            val key = KafkaKey(java.util.UUID.randomUUID.toString, 1).dump
            val res = MiddlewareResults(f(mkData)).dump
            val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, key, res)
            producer.send(record)
            try {
              Thread.sleep(100)
              sendMessage
            } catch {
              case e: InterruptedException => {
                producer.close
              }
            }
          }
          sendMessage
        }
      }
      thread.start
      return thread
    }
  }

  it should "correctly serialize and deserialize data" in {
    import edu.rpi.cs.nsl.v2v.spark.streaming.Serialization
    val kafkaKey = KafkaKey("abc", 10)
    val res = MiddlewareResults(12)

    val keyLoaded = Serialization.load[KafkaKey](kafkaKey.dump)
    val resLoaded = Serialization.load[MiddlewareResults](res.dump)
  }

  /**
   * Wait for a period of time to allow spark to process streaming data, then close publisher thread
   */
  def waitAndClose(thread: Thread) {
    val streamTime = 60 * 1000
    ssc.start
    ssc.awaitTerminationOrTimeout(streamTime)
    thread.interrupt
    System.err.println("Joining thread")
    thread.join(streamTime * 2)
  }

  it should "perform a map and sum" in new InitializedStream {
    val publisherThread = publishData(_.mph)
    vStream
      .map(_.mph)
      .reduce(_ + _, OperationIds.sum)
      .foreachRDD(_.foreach(System.err.println))
    waitAndClose(publisherThread)
  }

  it should "map/reduce to a tuple" in new InitializedStream {
    val publisherThread = publishData(vehicle => (vehicle.color, vehicle.mph))
    vStream
      .map(vehicle => (vehicle.color, vehicle.mph)) //TODO: determine how to make mapFunc serializable (currently anonymous functions work)
      .reduce((a, b) => a, OperationIds.filter) // Drop one tuple
      .foreachRDD(_.foreach(System.err.println))
    waitAndClose(publisherThread)
  }

  it should "implement reduceByKey" in new InitializedStream {
    val publisherThread = publishData(vehicle => (vehicle.color, vehicle.mph))
    vStream
      .map(vehicle => (vehicle.color, vehicle.mph))
      .reduceByKey(_ + _, OperationIds.sum)
      .foreachRDD(_.foreach(System.err.println))
    waitAndClose(publisherThread)
  }

  /**
   * Cleanup Code
   *
   * MUST be run after all tests
   */
  after {
    cleanAfter(this.ssc)
    this.stopContainers
  }
}