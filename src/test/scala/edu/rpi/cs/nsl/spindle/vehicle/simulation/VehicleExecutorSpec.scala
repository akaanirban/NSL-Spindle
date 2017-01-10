package edu.rpi.cs.nsl.spindle.vehicle.simulation

import org.scalatest.FlatSpec
import org.slf4j.LoggerFactory
import scala.util.Random
import edu.rpi.cs.nsl.spindle.vehicle.Types.Timestamp
import scala.concurrent.Future

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
  
  //TODO: create, run vehicle, ensure it generates expected outputs
  //TODO: test having lots of vehicles running at once
}