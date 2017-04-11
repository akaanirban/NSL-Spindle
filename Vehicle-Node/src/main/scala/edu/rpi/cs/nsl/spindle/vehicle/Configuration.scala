package edu.rpi.cs.nsl.spindle.vehicle

import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._

/**
  * Created by wrkronmiller on 4/4/17.
  */
object Configuration {
  protected val conf = ConfigFactory.load()
  //TODO: load local kafka and zookeeper configs, figre out how we get clusterhead information (maybe just fire data at local port that we assume sends data to clusterhead)
    // maybe just continue publishing messages destined for clusterhead to ch-output topic

  lazy val nodeId: Long = conf.getLong("spindle.vehicle.id")

  object Local {
    lazy val zkString = conf.getString("local.zookeeper.connection-string")
    lazy val kafkaBrokers = conf.getString("local.kafka.brokers")
  }

  object Kafka {
    lazy val testTopicName = conf.getString("spindle.vehicle.kafka.test-topic.name")
  }

  object Zookeeper {
    val connectTimeoutMs = 1000
    val sessionTimeoutMs = 10000
  }

  object Streams {
    val maxBatchSize = 9500
    val commitMs = 500
    val maxBufferRecords = 2
    val pollMs = (1 seconds).toMillis
    val sessionTimeout = (6 seconds).toMillis
    val reduceWindowSizeMs: Long = conf.getLong("spindle.streams.reduce.window.ms")
  }
}
