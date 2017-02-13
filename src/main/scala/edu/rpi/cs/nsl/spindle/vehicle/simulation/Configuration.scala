package edu.rpi.cs.nsl.spindle.vehicle.simulation

import java.io.File

import scala.collection.JavaConverters.asScalaBufferConverter
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.postgres.PgDefaults

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
  lazy val zkString = conf.getString("zookeeper.connection.string")

  object Postgres {
    val host = conf.getString("postgres.hostname")
    val username = conf.getOpt[String]("postgres.username").getOrElse(PgDefaults.username)
    val password = conf.getString("postgres.password")
    val port = conf.getOpt[Int]("postgres.port").getOrElse(PgDefaults.port)
    val database = conf.getOpt[String]("postgres.database").getOrElse(PgDefaults.database)
    val ssl = conf.getOpt[Boolean]("postgres.ssl").getOrElse(PgDefaults.ssl)
    val readOnly = conf.getOpt[Boolean]("postgres.readOnly").getOrElse(PgDefaults.readOnly)
  }

  val simStartOffsetMs = 5 * 1000 + (100 * Vehicles.maxEnabledNodes)
  // Uniquely identifies the current job
  val simUid = conf.getOpt[String]("simulation.uid").getOrElse(java.util.UUID.randomUUID.toString)

  private val resultsRoot = "simulation-results"
  private lazy val resultsName = s"${Vehicles.clusterMemberTable}_${Vehicles.maxEnabledNodes}_${System.currentTimeMillis}"
  val simResultsDir: String =  {
    val path = s"$resultsRoot/$resultsName"
    val file = new File(path)
    if(file.exists() == false) {
      assert(file.mkdirs(), s"Failed to make directory $path")
    }
    path
  }
  val simResultsFinishedDir: String = conf.getOpt[String]("simulation.results.dir") match {
    case None => {
      val path = s"$resultsRoot/completed"
      val file = new File(path)
      if (file.exists == false) {
        file.mkdirs()
      }
      s"$path/${Vehicles.clusterMemberTable}_${Vehicles.maxEnabledNodes}_${System.currentTimeMillis}"
    }
    case Some(path) => {
      val file = new File(path)
      file.mkdirs()
      path
    }
  }
  val simReportSeconds = 10

  object Streams {
    val commitMs = 500
    val maxBufferRecords = 100
    val pollMs = (1 seconds).toMillis
    val sessionTimeout = (6 seconds).toMillis
    val reduceWindowSizeMs: Long = conf.getLong("spindle.sim.streams.reduce.window.ms")
  }

  object Vehicles {
    private val vehiclePrefix = "spindle.sim.vehicle"
    val shutdownTimeout: FiniteDuration = (5 minutes)

    val shutdownReducersWhenComplete: Boolean = false //TODO: ensure each vehicle's clusterhead remains online
    val maxIterations: Option[Int] = conf.getOpt[Int](s"$vehiclePrefix.max.iterations")

    val clusterMemberTable: String = conf.getString(s"$vehiclePrefix.cluster.member.table")
    val maxEnabledNodes: Int = conf.getInt(s"$vehiclePrefix.max.vehicles")
    val eventsPerSecondMod = 1

    val mapReduceConfigName: String = conf.getString("spindle.sim.vehicle.mapreduce.config.name")

    object Sensors {
      private val prefix = s"$vehiclePrefix.sensors"
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
