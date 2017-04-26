package edu.rpi.cs.nsl.spindle

object Configuration {
  //TODO: load config from file/props
  object Zookeeper {
    lazy val connectString = "127.0.0.1:2181/nsl/v2v" //TODO: find uses
    lazy val sessionTimeoutMS = 3000
    lazy val registrationRoot = "/nsl/queries"
  }
}