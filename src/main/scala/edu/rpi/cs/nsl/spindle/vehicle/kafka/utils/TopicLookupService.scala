package edu.rpi.cs.nsl.spindle.vehicle.kafka.utils

trait TopicLookupService {
  type NodeId = Int
  private def getNodePrefix(node: NodeId) = s"vehicle-stream-$node"
  private def mkTopic(node: NodeId, suffix: String) = s"${getNodePrefix(node)}-$suffix"
  /**
   * Get output topic for a given vehicle (from which messages would be batched and transmitted)
   */
  private def getOutTopic(node: NodeId) = s"${getNodePrefix(node)}-output"
  /**
   * Get output topic for a given mapper on a given vehicle
   */
  def getMapperOutput(node: NodeId, mapperId: String) = mkTopic(node, s"mapper-$mapperId")
  def getReducerOutput(node: NodeId, reducerId: String) = mkTopic(node, s"reducer-$reducerId")
  def getSensorOutput(node: NodeId, sensorId: String) = mkTopic(node, s"sensor-$sensorId")
  /**
   * Get topic for a stream of windowed and joined sensor data
   *
   * @todo - create joiner
   */
  def getJoinedSensorOutput(node: NodeId, sensorIds: Iterable[String]) = {
    mkTopic(node, s"joined-${sensorIds.toSeq.sortBy(_.hashCode).reduce((a, b) => s"$a-$b")}")
  }

  /**
   * Get topic for cluster head "received messages"
   */
  def getClusterInput(node: NodeId) = mkTopic(node, s"-ch-input")
  def getClusterOutput(node: NodeId) = mkTopic(node, s"-ch-output")
}

object TopicLookupService {}