package edu.rpi.cs.nsl.spindle.vehicle.kafka.utils

trait TopicLookupService {
  type NodeId = Int
  private val globalPrefix = s"spindle-vehicle"
  private def mkTopic(suffix: String): String = s"$globalPrefix-$suffix"
  /**
   * Get output topic for a given mapper on a given vehicle
    *
    * @note all mappers publish to a shared output topic
   */
  def getMapperOutput: String = mkTopic(s"mapper-outputs")
  def getReducerOutput: String = mkTopic(s"reducer-outputs")
  // Mapper input
  def getVehicleStatus: String = mkTopic("vehicle-status")

  /**
   * Get topic for cluster head "received messages"
   */
  def getClusterInput: String = mkTopic("ch-input")

  lazy val middlewareInput = s"$globalPrefix-middleware-input"
}

object TopicLookupService extends TopicLookupService {}