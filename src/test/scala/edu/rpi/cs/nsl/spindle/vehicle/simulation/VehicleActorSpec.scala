package edu.rpi.cs.nsl.spindle.vehicle.simulation

import scala.util.Random

import org.scalatest.FlatSpec
import org.slf4j.LoggerFactory

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.WordSpecLike
import akka.testkit.TestActorRef

trait VehicleExecutorFixtures {
  private type TimeSeq = Seq[edu.rpi.cs.nsl.spindle.vehicle.Types.Timestamp]
  private val random = new Random()

  val FIVE_SECONDS_MS: Double = 5 * 1000 toDouble
  val startTime: Double = System.currentTimeMillis() + FIVE_SECONDS_MS
  val randomTimings: TimeSeq = 0 to 500 map (_ * random.nextGaussian())
  //TODO

  implicit class ComparableDouble(double: Double) {
    val THRESHOLD = 1E-5
    def approxEquals(other: Double): Boolean = {
      Math.abs(other) - Math.abs(double) < THRESHOLD
    }
  }
}

class VehicleActorSpec extends TestKit(ActorSystem("VehicleActorSpec")) with WordSpecLike with BeforeAndAfterAll {
  private val logger = LoggerFactory.getLogger(this.getClass)
  override def afterAll {
    shutdown()
  }
  "A Vehicle actor" should {
    "correctly generate timings" in new VehicleExecutorFixtures {
      val vExec = TestActorRef(new Vehicle(0, null, randomTimings, null, null, null)).underlyingActor
      //val vExec = system.actorOf(Vehicle.props(0, null, randomTimings, null, null, null))
      val timings = vExec.mkTimings(startTime)
      assert(timings.min == startTime)
      assert(timings.max approxEquals (randomTimings.max + startTime), s"${timings.max} != ${randomTimings.max + startTime}")
    }
  }

  //TODO: create, run vehicle, ensure it generates expected outputs
  //TODO: test having lots of vehicles running at once
}