package edu.rpi.cs.nsl.spindle.vehicle.kafka.executors

import edu.rpi.cs.nsl.spindle.vehicle.connections.Server

/**
  * Container for all information required to access a topic across Kafka clusters
  *
  * @param topic
  * @param connectionInfo
  */
case class GlobalTopic(topic: String, connectionInfo: KafkaConnectionInfo) {
  def toTuple: (KafkaConnectionInfo, String) = {
    (connectionInfo, topic)
  }
}

/**
  * Factory for GlobalTopic objects
  */
object GlobalTopic {
  import edu.rpi.cs.nsl.spindle.vehicle.Configuration.{Cloud, Local, Vehicle}
  private implicit class ServerList(serverList: List[Server]) {
    def getConnectionString: String = {
      serverList.map(_.getConnectionString).mkString(",")
    }
  }
  def mkCloudTopic(topic: String): GlobalTopic = {
    import Cloud.zkString
    import Cloud.kafkaBrokers
    GlobalTopic(topic, KafkaConnectionInfo(zkString, brokerString = kafkaBrokers.getConnectionString))
  }
  def mkLocalTopic(topic: String): GlobalTopic = {
    import Local.kafkaBroker
    import Local.zkString
    GlobalTopic(topic, KafkaConnectionInfo(zkString, brokerString = kafkaBroker.getConnectionString))
  }
  def mkGlobalTopic(topic: String, kafkaConnectionInfo: KafkaConnectionInfo): GlobalTopic = {
    GlobalTopic(topic, kafkaConnectionInfo)
  }
}