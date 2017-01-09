package edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.postgres

import java.sql.Connection
import java.sql.DriverManager
import java.util.Properties

import edu.rpi.cs.nsl.spindle.vehicle.PropUtils._
import edu.rpi.cs.nsl.spindle.vehicle.simulation.Configuration
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.EventStore
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.TSEntry
import edu.rpi.cs.nsl.spindle.vehicle.simulation.event_store.TimeSeriesQuery

object PgDefaults {
  val port = 5432
  val username = "postgres"
  val ssl = false
  val database = "postgres"
  val readOnly = true

  lazy val config = {
    import Configuration.Postgres
    PgConfig(Postgres.host, Postgres.port, Postgres.password, Postgres.username, Postgres.database, Postgres.ssl, Postgres.readOnly)
  }
}

case class PgConfig(host: String, port: Int, password: String, username: String, database: String, ssl: Boolean, readOnly: Boolean) {
  def getProps: Properties = {
    new Properties()
      .withPut("user", username)
      .withPut("password", password)
      .withPut("ssl", ssl.toString)
      .withPut("readOnly", readOnly.toString)
  }
}

class PgClient(config: PgConfig = PgDefaults.config) extends EventStore {
  Class.forName("org.postgresql.Driver") // REQUIRED
  private val uri = s"jdbc:postgresql://${config.host}:${config.port}/${config.database}"
  private lazy val connection = DriverManager.getConnection(uri, config.getProps)
  def close = connection.close
  def isReadOnly = connection.isReadOnly
  
  private lazy val readingQuery = new TimeSeriesQuery(connection)
  def getReadings(nodeId: Int): Stream[TSEntry] = readingQuery.loadReadings(nodeId)
}