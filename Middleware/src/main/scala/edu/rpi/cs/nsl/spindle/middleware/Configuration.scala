package edu.rpi.cs.nsl.spindle.middleware

import com.typesafe.config.ConfigFactory

/**
  * Created by wrkronmiller on 3/7/17.
  */
object Configuration {
  private val conf = ConfigFactory.load()
  lazy val zkString = conf.getString("zookeeper.connection.string")
  lazy val zkConnectionTimeout = conf.getInt("zookeeper.connection.timeout")
  lazy val zkSessionTimeout = conf.getInt("zookeeper.session.timeout")

  object ZkPaths {
    val root = "/middleware"
    val queries = s"$root/queries"
  }
}
