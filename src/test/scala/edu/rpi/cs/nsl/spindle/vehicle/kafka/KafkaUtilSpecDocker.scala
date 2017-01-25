package edu.rpi.cs.nsl.spindle.vehicle.kafka

import scala.concurrent.Await

import org.scalatest.BeforeAndAfterAll
import org.scalatest.FlatSpec
import org.slf4j.LoggerFactory
import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.KafkaAdmin
import edu.rpi.cs.nsl.spindle.vehicle.TestingConfiguration
import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.KafkaConfig
import edu.rpi.cs.nsl.spindle.tags.UnderConstructionTest

class KafkaUtilSpecDocker extends FlatSpec with BeforeAndAfterAll {
  import Constants._
  private val logger = LoggerFactory.getLogger(this.getClass)

  private lazy val zkString = DockerHelper.getZkString
  private lazy val kafkaAdmin = new KafkaAdmin(zkString)

  protected lazy val kafkaConfig: KafkaConfig = DockerHelper.getKafkaConfig

  override def beforeAll {
    logger.info("Requesting kafka cluster from docker")
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
    logger.info("Requesting kafka cluster shutdown")
    DockerHelper.stopCluster
  }
}