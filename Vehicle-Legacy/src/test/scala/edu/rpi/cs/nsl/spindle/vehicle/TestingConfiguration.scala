package edu.rpi.cs.nsl.spindle.vehicle

import org.scalatest.DoNotDiscover
import scala.sys.process._
import java.net.InetAddress
import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config
import scala.collection.JavaConversions._
import com.typesafe.config.ConfigObject
import edu.rpi.cs.nsl.spindle.vehicle.simulation.ConfigurationSingleton

@DoNotDiscover
object TestingConfiguration extends ConfigurationSingleton {
  //val hostname = "butterfly-68.dynamic2.rpi.edu"
  lazy val dockerHost = {
    val ipAddr = "scripts/kafka-docker/get-ip.sh".!!.trim
    // Ensure IP is valid
    InetAddress.getByName(ipAddr).getAddress
    ipAddr
  }

  val shutdownDockerWhenDone = false

  val numVehicles = 3340
}