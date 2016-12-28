package edu.rpi.cs.nsl.spindle.vehicle

import org.scalatest.DoNotDiscover
import scala.sys.process._

@DoNotDiscover
object Configuration {
  //val hostname = "butterfly-68.dynamic2.rpi.edu"
  val hostname = "scripts/kafka-docker/get-ip.sh".!!.trim
}