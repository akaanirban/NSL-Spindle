package edu.rpi.cs.nsl.spindle.vehicle.kafka.streams

import java.util.Properties

import org.apache.kafka.streams.StreamsConfig
import org.slf4j.LoggerFactory
import _root_.edu.rpi.cs.nsl.spindle.vehicle.Configuration
import edu.rpi.cs.nsl.spindle.vehicle.connections.Server

class StreamConfigException(message: String) extends RuntimeException(message)

/**
 * Build StreamsConfig with pipelined methods
 */
case class StreamsConfigBuilder(properties: Properties = new Properties()) {
  import StreamsConfig._

  private val logger = LoggerFactory.getLogger(this.getClass)
  def withProperty(key: String, value: String): StreamsConfigBuilder = {
    val newProps = new Properties()
    newProps.putAll(properties)
    newProps.put(key, value)
    this.copy(newProps)
  }
  /**
   * Ensure streams Id uses only valid characters
   *
   * @see [[http://docs.confluent.io/3.0.0/streams/developer-guide.html#required-configuration-parameters Required Configuration Parameters]]
   */
  private def idIsValid(id: String): Boolean = {
    val validRegex = "[a-z|A-Z|0-9|\\.|\\-|_]+".r
    id match {
      case validRegex(_*) => true
      case _              => false
    }
  }
  private def assertIdIsValid(id: String) {
    if (idIsValid(id) == false) {
      throw new StreamConfigException(s"$id is not a valid streams Id")
    }
  }
  def withId(id: String): StreamsConfigBuilder = {
    assertIdIsValid(id)
    this.withProperty(APPLICATION_ID_CONFIG, id)
  }
  /**
   * Generate ID from name and semantic version using recommended format
   */
  def withId(name: String, major: Int, minor: Int, patch: Int): StreamsConfigBuilder = {
    withId(s"name-v.$major.$minor.$patch")
  }
  def withServers(bootstrapServers: String): StreamsConfigBuilder = {
    logger.info(s"Using servers $bootstrapServers with key $BOOTSTRAP_SERVERS_CONFIG")
    this.withProperty(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  }
  def withServers(bootstramServers: Iterable[Server]): StreamsConfigBuilder = {
    this.withServers(bootstramServers.map(_.toString).mkString(","))
  }
  def withZk(zkString: String): StreamsConfigBuilder = this.withProperty(ZOOKEEPER_CONNECT_CONFIG, zkString)
  def withCommitInterval(intervalMs: Long = Configuration.Streams.commitMs): StreamsConfigBuilder = this.withProperty(COMMIT_INTERVAL_MS_CONFIG, intervalMs.toString)
  def withAutoOffset(resetLocation: String = "latest"): StreamsConfigBuilder = this.withProperty("auto.offset.reset", resetLocation)
  def withMaxRecords(maxRecords: Int = Configuration.Streams.maxBufferRecords) = this.withProperty(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, maxRecords.toString)
  def withPollMs(pollMs: Long = Configuration.Streams.pollMs) = this.withProperty("poll.ms", pollMs.toString)
  def withSessionTimeoutMs(timeoutMs: Long = Configuration.Streams.sessionTimeout) = this.withProperty("session.timeout.ms", timeoutMs.toString)
  def withBatchSize(batchSize: Long = Configuration.Streams.maxBatchSize) = this.withProperty("batch.size", batchSize.toString)
  def withDefaults = {
    this.withCommitInterval()
      .withMaxRecords()
      .withPollMs()
      .withSessionTimeoutMs()
  }
  def build: StreamsConfig = new StreamsConfig(properties)
}