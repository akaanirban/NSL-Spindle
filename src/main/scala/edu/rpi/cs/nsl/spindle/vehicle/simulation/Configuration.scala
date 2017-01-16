package edu.rpi.cs.nsl.spindle.vehicle.simulation

import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.postgres.PgDefaults

import scala.collection.JavaConverters.asScalaBufferConverter

trait ConfigurationSingleton {
  protected val conf = ConfigFactory.load()
  protected implicit class EnhancedConfig(config: Config) {
    def getOpt[T](key: String): Option[T] = {
      config.hasPathOrNull(key) match {
        case false => None
        case true  => Some(config.getAnyRef(key).asInstanceOf[T])
      }
    }
    def getDoubleList(key: String): List[Double] = {
      config.getStringList(key).asScala.map(_.toDouble).toList
    }
  }
}

/**
 * Simulator configuration settings
 *
 * @note not an object, so that it is easier to run unit tests and to run multiple tests in parallel
 */
object Configuration extends ConfigurationSingleton {
  lazy val kafkaServers = conf.getString("kafka.brokers")

  object Postgres {
    val host = conf.getString("postgres.hostname")
    val username = conf.getOpt[String]("postgres.username").getOrElse(PgDefaults.username)
    val password = conf.getString("postgres.password")
    val port = conf.getOpt[Int]("postgres.port").getOrElse(PgDefaults.port)
    val database = conf.getOpt[String]("postgres.database").getOrElse(PgDefaults.database)
    val ssl = conf.getOpt[Boolean]("postgres.ssl").getOrElse(PgDefaults.ssl)
    val readOnly = conf.getOpt[Boolean]("postgres.readOnly").getOrElse(PgDefaults.readOnly)
  }

  val simStartOffsetMs = 5000

  object Vehicles {
    object Sensors {
      private val prefix = "spindle.sim.vehicle.sensors"
      //TODO: load json, use beans, etc...
      val singleValSensors = {
        val singleValNames: List[String] = conf.getStringList(s"$prefix.singlevaluesensor.names").asScala.toList
        val singleValValues = conf.getDoubleList(s"$prefix.singlevaluesensor.values").asScala.toList
        singleValNames.zip(singleValValues)
      }
      val rngSensors = {
        val rngValueNames: List[String] = conf.getStringList(s"$prefix.rngvaluesensor.names").asScala.toList
        val rngValueMaxVals = conf.getDoubleList(s"$prefix.rngvaluesensor.maxvals").asScala.toList
        rngValueNames.zip(rngValueMaxVals)
      }
    }
  }
}