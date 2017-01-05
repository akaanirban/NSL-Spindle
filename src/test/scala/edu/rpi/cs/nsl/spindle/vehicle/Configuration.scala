package edu.rpi.cs.nsl.spindle.vehicle

import org.scalatest.DoNotDiscover
import scala.sys.process._
import java.net.InetAddress

@DoNotDiscover
object Configuration {
  //val hostname = "butterfly-68.dynamic2.rpi.edu"
  lazy val dockerHost = {
    val ipAddr = "scripts/kafka-docker/get-ip.sh".!!.trim
    // Ensure IP is valid
    InetAddress.getByName(ipAddr).getAddress
    ipAddr
  }
  
}