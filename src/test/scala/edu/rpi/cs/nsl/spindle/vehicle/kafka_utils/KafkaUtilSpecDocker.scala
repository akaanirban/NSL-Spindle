package edu.rpi.cs.nsl.spindle.vehicle.kafka_utils

import scala.concurrent.Await
import org.scalatest.BeforeAndAfterAll
import edu.rpi.cs.nsl.spindle.vehicle.Configuration
import org.scalatest.FlatSpec
import org.slf4j.LoggerFactory
import scala.concurrent.duration._

class KafkaUtilSpecDocker extends FlatSpec with BeforeAndAfterAll {
  import Constants._
  private val logger = LoggerFactory.getLogger(this.getClass)

  private lazy val kafkaAdmin = new KafkaAdmin(s"${Configuration.dockerHost}:2181")

  protected lazy val kafkaConfig: KafkaConfig = {
    val servers = DockerHelper.getPorts.kafkaPorts
      .map(a => s"${Configuration.dockerHost}:$a")
      .reduceOption((a, b) => s"$a,$b") match {
        case Some(servers) => servers
        case None          => throw new RuntimeException(s"No kafka servers found on host ${Configuration.dockerHost}")
      }
    KafkaConfig()
      .withServers(servers)
  }

  override def beforeAll {
    logger.info("Resetting kafka cluster") //TODO
    DockerHelper.stopCluster
    DockerHelper.startCluster
    logger.info(s"Waiting for kafka to converge")
    Await.ready(kafkaAdmin.waitBrokers(DockerHelper.NUM_KAFKA_BROKERS), KAFKA_WAIT_TIME)
    //Thread.sleep((1 minutes).toMillis) //TODO: clean up
    logger.info("Done waiting")
  }

  private lazy val sharedTests = new KafkaSharedTests(kafkaConfig, kafkaAdmin)

  it should "create a producer" in {
    sharedTests.mkProducer
  }

  it should "send an object without crashing" in {
    logger.info("Testing send/recv")
    sharedTests.testSendRecv
    logger.info("Done testing send/recv")
  }

  ignore should "produce data from a data source" in {
    //TODO
    fail("Not implemented")
  }

  override def afterAll {
    kafkaAdmin.close
    logger.info("Shutting down kafka cluster")
    DockerHelper.stopCluster
  }
}