package edu.rpi.cs.nsl.spindle.vehicle.kafka.streams

package edu.rpi.cs.nsl.spindle.vehicle.simulation

import java.util.concurrent.ConcurrentMap
import java.util.concurrent.ConcurrentHashMap
import org.slf4j.LoggerFactory

//TODO
object MetricsCache {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val dataLog: ConcurrentMap[String, Long] = new ConcurrentHashMap[String, Long]()
  def addBytes(destTopic: String, numBytes: Long) {
    val totalBytes = dataLog.compute(destTopic, (k, v) => numBytes + v)
    logger.info(s"Topic $destTopic has received a total of $totalBytes bytes")
  }
}