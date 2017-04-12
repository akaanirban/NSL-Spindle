package edu.rpi.cs.nsl.spindle.vehicle.connections

import edu.rpi.cs.nsl.spindle.vehicle.Configuration
import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.{ConsumerKafka, KafkaAdmin, KafkaConfig, SingleTopicProducerKakfa}
import org.slf4j.LoggerFactory

import scala.concurrent.{Await, Future, blocking}
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by wrkronmiller on 4/5/17.
  */
class KafkaConnection(brokers: Iterable[Server], zkString: String) extends Connection[KafkaAdmin] {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val config = KafkaConfig().withServers(brokers.map(_.getConnectionString).mkString(","))

  def close: KafkaConnection = {
    this
  }

  private def testSendRecv(topic: String): Future[Unit] = {
    val consumer = new ConsumerKafka[String, String](config.withConsumerDefaults.withConsumerGroup("testSendRecv"))
    consumer.subscribe(topic)

    val testKV = java.util.UUID.randomUUID().toString
    def waitMessage(): Unit = {
      val matches = consumer.getMessages.filter(msg => msg._1 == testKV && msg._2 == testKV)
      if(matches.size != 1) {
        waitMessage
      }
    }

    val producer = new SingleTopicProducerKakfa[String, String](topic, config.withProducerDefaults)
    producer
      .send(testKV, testKV)
      .flatMap(_ => Future { blocking { waitMessage() }})
      .map(_ => producer.close)
      .map(_ => consumer.close)
  }

  private def mkTestTopic(admin: KafkaAdmin): Future[Unit] = {
    admin
      .mkTopic(Configuration.Kafka.testTopicName)
      .flatMap(_ => testSendRecv(Configuration.Kafka.testTopicName))
      .map(_ => logger.info("Successfully initialized test topic"))
  }

  def openAsync: Future[KafkaConnection] = {
    val admin = new KafkaAdmin(zkString)
    admin.waitBrokers(brokers.size)
      .flatMap(_ => mkTestTopic(admin))
      .map(_ => admin.close)
      .map(_ => this)
  }

  def openSync(timeout: Duration): KafkaConnection = {
    Await.result(openAsync, timeout)
  }

  def getAdmin: KafkaAdmin = {
    new KafkaAdmin(zkString)
  }
}
