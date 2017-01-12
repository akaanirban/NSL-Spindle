package edu.rpi.cs.nsl.spindle.vehicle.simulation

import org.scalatest.FlatSpec
import org.slf4j.LoggerFactory
import scala.util.Random
import edu.rpi.cs.nsl.spindle.vehicle.Types.Timestamp
import scala.concurrent.Future
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

trait VehicleExecutorFixtures {
  private val random = new Random()

  val FIVE_SECONDS_MS: Double = 5 * 1000 toDouble
  val startTime: Double = System.currentTimeMillis() + FIVE_SECONDS_MS
  val randomTimings: Seq[Timestamp] = 0 to 500 map (_ * random.nextGaussian())
  //TODO
  lazy val timingConfig = TimingConfig(randomTimings, Future.successful(startTime))

  implicit class ComparableDouble(double: Double) {
    val THRESHOLD = 1E-5
    def approxEquals(other: Double): Boolean = {
      Math.abs(other) - Math.abs(double) < THRESHOLD
    }
  }
}

class VehicleExecutorSpec extends FlatSpec {
  private val logger = LoggerFactory.getLogger(this.getClass)
  it should "correctly generate timings" in new VehicleExecutorFixtures {
    val vExec = new Vehicle(0, null, timingConfig, null, null, null)
    val timings = vExec.mkTimings(startTime)
    assert(timings.min == startTime)
    assert(timings.max approxEquals (randomTimings.max + startTime), s"${timings.max} != ${randomTimings.max + startTime}")
  }
  
  val NUM_THREADS = 4000
  it should "be able to spawn child threads" in {
    val RUN_MS = 10000
    val pool = Executors.newFixedThreadPool(NUM_THREADS)
    val parentThreads = (0 to 4000).map(nodeId => new Thread(){
      override def run {
        val innerPool = Executors.newCachedThreadPool()
        logger.debug(s"Test thread $nodeId starting")
        val innerThreads = (0 to 100).map(taskId => new Thread(){
          override def run {
            logger.debug(s"Task $taskId thread $nodeId started")
            def loop {
              val looping: Boolean = 
              try {
                Thread.sleep(500)
                true
              } catch {
                case e: InterruptedException => false
              }
              if(looping && Thread.interrupted() == false){
                loop
              }
            }
          }
        })
        innerThreads.foreach(innerPool.execute)
        Thread.sleep(RUN_MS)
        innerThreads.foreach(_.interrupt)
        innerPool.shutdown()
        innerPool.awaitTermination(10, TimeUnit.SECONDS)
      }
    })
    parentThreads.foreach(pool.execute)
    pool.awaitTermination(RUN_MS, TimeUnit.MILLISECONDS)
  }
  
  //TODO: create, run vehicle, ensure it generates expected outputs
  //TODO: test having lots of vehicles running at once
}