package edu.rpi.cs.nsl.spindle.vehicle.data_sources

import edu.rpi.cs.nsl.spindle.vehicle.kafka_utils.KafkaConfig
import edu.rpi.cs.nsl.spindle.vehicle.kafka_utils.ProducerKafka

case class DataSourceKey() extends Serializable //TODO: routing/partitioning info

class DataProducer[V](servers: String) extends ProducerKafka[DataSourceKey, V](KafkaConfig().withProducerDefaults.withServers(servers))

/**
 * Pushes sensor data onto Kafka
 */
class DataSource[T](name: String, producer: DataProducer[T]) {
  private val topic = s"data-source-$name"
  private val key = DataSourceKey() //TODO: finish implementing DataSource

  def send(reading: T) = {
    producer.send(topic, key, reading)
  }
}