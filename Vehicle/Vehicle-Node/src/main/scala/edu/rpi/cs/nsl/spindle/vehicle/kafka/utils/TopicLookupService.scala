package edu.rpi.cs.nsl.spindle.vehicle.kafka.utils

trait TopicLookupService {
  type NodeId = Int
  private val globalPrefix = s"spindle-vehicle"
  private def mkTopic(suffix: String): String = s"$globalPrefix-$suffix"
  /**
   * Get output topic for a given mapper on a given vehicle
   */
  def getMapperOutput(mapperId: String): String = mkTopic(s"mapper-$mapperId")
  def getReducerOutput(reducerId: String): String = mkTopic(s"reducer-$reducerId")
  // Mapper input
  def getVehicleStatus: String = mkTopic("vehicle-status")

  /**
   * Get topic for cluster head "received messages"
   */
  def getClusterInput: String = mkTopic("ch-input")
  // Topic for outputting to cloud
  def getClusterOutput(node: NodeId): String = middlewareInput

  lazy val middlewareInput = s"$globalPrefix-middleware-input"
}

object TopicLookupService extends TopicLookupService {}