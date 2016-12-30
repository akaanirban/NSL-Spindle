package edu.rpi.cs.nsl.spindle.vehicle.kafka_utils

import edu.rpi.cs.nsl.spindle.vehicle.cloud.TerraformManager
import edu.rpi.cs.nsl.spindle.vehicle.Scripts
import scala.concurrent._

case class ServerList(zookeeper: String, brokers: Iterable[String])

object TerraformUtils extends TerraformManager(s"${Scripts.SCRIPTS_DIR}/kafka-terraform") {
  private object OutputKeys {
    val kafka = "kafka"
    val zookeeper = "zookeeper"
  }
  private def getServers(outputMap: Map[String, String]): ServerList = {
    val zookeeper: String = outputMap
      .get(OutputKeys.zookeeper)
      .get
    val kafka: Iterable[String] = outputMap
      .get(OutputKeys.kafka)
      .get
      .split(",")
      .map(_.trim)
    ServerList(zookeeper, kafka)
  }
  def getServers: Future[ServerList] = {
    implicit val ec = ExecutionContext.global
    getOutputMap.map(getServers(_))
  }
}