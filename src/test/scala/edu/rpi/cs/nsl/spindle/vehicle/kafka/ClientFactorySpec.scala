package edu.rpi.cs.nsl.spindle.vehicle.kafka

import org.scalatest.FlatSpec
import scala.concurrent.Await
import org.scalatest.BeforeAndAfterAll
import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.KafkaAdmin

class ClientFactorySpecDocker extends FlatSpec with BeforeAndAfterAll {
  private lazy val zkString = DockerHelper.getZkString
  private lazy val kafkaAdmin = new KafkaAdmin(zkString)
  private lazy val baseConfig = DockerHelper.getKafkaConfig
  private lazy val streamsTestFixtures = new KafkaStreamsTestFixtures(baseConfig, kafkaAdmin, zkString)

  override def beforeAll {
    DockerHelper.startCluster
    Await.ready(kafkaAdmin.waitBrokers(DockerHelper.NUM_KAFKA_BROKERS), Constants.KAFKA_WAIT_TIME)
  }

  private def getFactory = {
    val TEST_STREAM_ID = java.util.UUID.randomUUID.toString
    val streamsConfig = streamsTestFixtures.getStreamsConfig(TEST_STREAM_ID)
    new ClientFactory(baseConfig, streamsConfig)
  }

  it should "make a valid factory" in {
    getFactory
  }

  it should "make a producer" in {
    val producer = getFactory.mkProducer
    producer.close
  }

  private def getRandString = java.util.UUID.randomUUID.toString
  
  it should "make a mapper" in {
    getFactory.mkMapper[Null, Null, Null, Null](getRandString, getRandString, (a, b) => null, getRandString)
  }

  it should "make a kv reducer" in {
    getFactory.mkKvReducer[Null, Null](getRandString, getRandString, (a, b) => null, getRandString)
  }
  
  it should "make a full reducer" in {
    getFactory.mkReducer[Null, Null](getRandString, getRandString, (a, b) => null, getRandString)
  }

  override def afterAll {
    kafkaAdmin.close
    DockerHelper.stopCluster
  }
}