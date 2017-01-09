package edu.rpi.cs.nsl.spindle.vehicle.kafka_utils

import scala.concurrent.Await

import org.scalatest.BeforeAndAfterAll
import org.scalatest.FlatSpec
import org.slf4j.LoggerFactory

import edu.rpi.cs.nsl.spindle.vehicle.TestingConfiguration
import edu.rpi.cs.nsl.spindle.vehicle.simulation.DockerHelper

class KafkaUtilSpecDocker extends FlatSpec with BeforeAndAfterAll {
  import Constants._
  private val logger = LoggerFactory.getLogger(this.getClass)

  private lazy val zkString = s"${TestingConfiguration.dockerHost}:2181"
  private lazy val kafkaAdmin = new KafkaAdmin(zkString)

  protected lazy val kafkaConfig: KafkaConfig = {
    val servers = DockerHelper.getPorts.kafkaPorts
      .map(a => s"${TestingConfiguration.dockerHost}:$a")
      .reduceOption((a, b) => s"$a,$b") match {
        case Some(servers) => servers
        case None          => throw new RuntimeException(s"No kafka servers found on host ${TestingConfiguration.dockerHost}")
      }
    KafkaConfig()
      .withServers(servers)
  }

  override def beforeAll {
    logger.info("Resetting kafka cluster")
    DockerHelper.stopCluster
    DockerHelper.startCluster
    logger.info(s"Waiting for kafka to converge")
    Await.ready(kafkaAdmin.waitBrokers(DockerHelper.NUM_KAFKA_BROKERS), KAFKA_WAIT_TIME)
    logger.info("Done waiting")
  }

  private lazy val sharedTests = new KafkaStreamsTestFixtures(kafkaConfig, kafkaAdmin, zkString)

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

  it should "perform map operations" in {
    sharedTests.testStreamMapper
  }

  it should "perform reduceByKey operations" in {
    sharedTests.testKVReducer
  }

  it should "perform a simple reduce operation" in {
    sharedTests.testFullReducer
  }

  override def afterAll {
    kafkaAdmin.close
    logger.info("Shutting down kafka cluster")
    DockerHelper.stopCluster
  }
}