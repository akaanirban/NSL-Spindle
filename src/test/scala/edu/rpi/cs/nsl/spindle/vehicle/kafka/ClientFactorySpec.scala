package edu.rpi.cs.nsl.spindle.vehicle.kafka

import org.scalatest.FlatSpec
import scala.concurrent.Await
import org.scalatest.BeforeAndAfterAll
import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.KafkaAdmin
import edu.rpi.cs.nsl.spindle.vehicle.simulation.Configuration

private[vehicle] object ClientFactoryDockerFixtures {
  lazy val zkString = DockerHelper.getZkString
  lazy val kafkaAdmin = new KafkaAdmin(zkString)
  lazy val baseConfig = DockerHelper.getKafkaConfig
  lazy val streamsTestFixtures = new KafkaStreamsTestFixtures(baseConfig, kafkaAdmin, zkString)
  def waitReady {
    DockerHelper.startCluster
    Await.ready(kafkaAdmin.waitBrokers(DockerHelper.NUM_KAFKA_BROKERS), Constants.KAFKA_WAIT_TIME)
  }
  def getFactory = {
    val TEST_STREAM_ID = java.util.UUID.randomUUID.toString
    val streamsConfig = streamsTestFixtures
      .getStreamsConfig(TEST_STREAM_ID)
      .withAutoOffset()
      .withCommitInterval(Configuration.Streams.commitMs)
    new ClientFactory(baseConfig, streamsConfig)
  }
}

class ClientFactorySpecDocker extends FlatSpec with BeforeAndAfterAll {
  import ClientFactoryDockerFixtures._

  override def beforeAll {
    waitReady
  }

  it should "make a valid factory" in {
    getFactory
  }

  it should "make a producer" in {
    val producer = getFactory.mkProducer(getRandString)
    producer.close
  }

  private def getRandString = java.util.UUID.randomUUID.toString

  it should "make a mapper" in {
    getFactory.mkMapper[None.type, None.type, None.type, None.type](getRandString, getRandString, (a, b) => (None, None), getRandString)
  }

  it should "make a kv reducer" in {
    getFactory.mkKvReducer[None.type, None.type](getRandString, getRandString, (a, b) => None, getRandString)
  }

  it should "make a full reducer" in {
    getFactory.mkReducer[None.type, None.type](getRandString, getRandString, (a, b) => None, getRandString)
  }

  override def afterAll {
    kafkaAdmin.close
    DockerHelper.stopCluster
  }
}