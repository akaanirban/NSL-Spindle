package edu.rpi.cs.nsl.spindle.vehicle.events

import edu.rpi.cs.nsl.spindle.datatypes.Vehicle
import edu.rpi.cs.nsl.spindle.vehicle.Configuration
import edu.rpi.cs.nsl.spindle.vehicle.Types.Timestamp
import edu.rpi.cs.nsl.spindle.vehicle.connections.KafkaConnection
import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.{SingleTopicProducerKakfa, TopicLookupService}
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by wrkronmiller on 4/12/17.
  */
trait SensorProducer extends TemporalDaemon[Unit] {}

/**
  * Combines SensorProducer with Kafka producer
  * @param kafkaConnection
  */
abstract class PublishingSensorProducer(kafkaConnection: KafkaConnection)
  extends SingleTopicProducerKakfa[Timestamp, Vehicle](topic=TopicLookupService.getVehicleStatus,
    config=kafkaConnection.getProducerConfig) with SensorProducer {
  private val logger = LoggerFactory.getLogger(this.getClass)
  /**
    * Write sensor data to Kafka
    * @param vehicle
    * @return
    */
  protected def publishKafka(timestamp: Timestamp, vehicle: Vehicle): Future[Unit] = {
    logger.debug(s"Sensor sending $vehicle for $timestamp")
    this.send(timestamp, vehicle).map(_ => Unit)
  }
}

/**
  * Sensor producer with statically configured values
  * @param kafkaConnection
  */
class FixedSensorProducer(kafkaConnection: KafkaConnection) extends PublishingSensorProducer(kafkaConnection) {
  // Load fixed values from central configuration
  import edu.rpi.cs.nsl.spindle.vehicle.Configuration.Vehicle.Sensors.fixedValues
  private val valueMap: Map[String, String] = fixedValues.map{case (k,v) => (k, v.get)}
  // Need to use new with case class overloaded constructor
  private val vehicle = new Vehicle(m=valueMap)
  /**
    * Load fixed sensor data and publish it to Kafka
    * @param currentTime
    * @return
    */
  override def executeInterval(currentTime: Timestamp): Future[Unit] = publishKafka(currentTime, vehicle)
  override def safeShutdown: Future[Unit] = Future.successful(Unit)
}

/**
  * Type of sensor producer to be used
  *
  * @note this enum is used by the main application Configuration
  */
object SensorType extends Enumeration {
  type SensorType = Value
  val External, Fixed = Value
  //TODO: random, database-based
}

/**
  * Sensor factory
  */
object SensorProducer {
  import SensorType._
  def load(kafkaConnection: KafkaConnection): SensorProducer = Configuration.Vehicle.Sensors.sensorType match {
    case External => throw new RuntimeException("TODO: external sensor handler")
    case Fixed => new FixedSensorProducer(kafkaConnection)
  }
}
