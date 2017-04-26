package edu.rpi.cs.nsl.spindle.vehicle.connections

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

/**
  * Created by wrkronmiller on 4/5/17.
  */
trait Connection[Admin] {
  def close: Connection[Admin]
  def openSync(timeout: Duration = Duration.Inf): Connection[Admin]
  def openAsync: Future[Connection[Admin]]
  def getAdmin: Admin
}

case class Server(hostname: String, port: Long) {
  def getConnectionString: String = {
    s"${hostname}:${port}"
  }
}

object Server {
  def fromString(string: String): Server = {
    val Array(hostname, portString) = string.split(":")
    Server(hostname, portString.toLong)
  }
  def fromListString(listString: String): Iterable[Server] = {
    listString.split(",").map(fromString)
  }
}
