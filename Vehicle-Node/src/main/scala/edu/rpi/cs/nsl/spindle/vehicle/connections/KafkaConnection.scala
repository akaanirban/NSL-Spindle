package edu.rpi.cs.nsl.spindle.vehicle.connections

import edu.rpi.cs.nsl.spindle.vehicle.Configuration
import edu.rpi.cs.nsl.spindle.vehicle.kafka.streams.StreamsConfigBuilder
import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils._
import org.apache.kafka.clients.producer.KafkaProducer
import org.slf4j.LoggerFactory

import scala.reflect.runtime.universe.TypeTag
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

  /**
    * Get configuration for a simple Kafka producer
    * @return new KafkaConfig with producer defaults
    */
  def getProducerConfig: KafkaConfig = config.withProducerDefaults

  /**
    * Get config builder for KafkaStreams executors
    * @return new configbuilder with default settings and zk string set
    */
  def getStreamsConfigBuilder: StreamsConfigBuilder = StreamsConfigBuilder()
    .withDefaults.withServers(brokers)
    .withZk(zkString)

  /**
    * Create A Kafka Producer
    * @tparam K Key Type
    * @tparam V Value Type
    * @return a new ProducerKafka instance
    */
  def getProducer[K: TypeTag, V: TypeTag]: ProducerKafka[K,V] = {
    new ProducerKafka[K,V](getProducerConfig)
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

    val producer = new SingleTopicProducerKakfa[String, String](topic, getProducerConfig)
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
      .flatMap{_ =>
        println("Creating test topic")
        mkTestTopic(admin)
      }
      .map{_ =>
        println("Created test topic")
        admin.close
      }
      .map(_ => this)
  }

  def openSync(timeout: Duration): KafkaConnection = {
    Await.result(openAsync, timeout)
  }

  def getAdmin: KafkaAdmin = {
    println(s"Creating admin $zkString")
    new KafkaAdmin(zkString)
  }
}

/**
  * Kafka connection factory
  */
object KafkaConnection {
  def getLocal: KafkaConnection = {
    import Configuration.Local.{kafkaBroker, zkString}
    new KafkaConnection(Seq(kafkaBroker), zkString)
  }
}
