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
import scala.util.{Failure, Success, Try}

/**
  * Created by wrkronmiller on 4/5/17.
  */
class KafkaConnection(brokers: Iterable[Server], zkString: String) extends Connection[KafkaAdmin] {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val config = KafkaConfig().withServers(brokers.map(_.getConnectionString).mkString(","))

  val MAX_INIT_MESSAGE_ATTEMPTS = 10

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
    println(s"Performing test send/recv on $topic")
    val consumer = new ConsumerKafka[String, String](config.withConsumerDefaults.withConsumerGroup("testSendRecv").withAutoOffsetReset("latest"))
    consumer.subscribe(topic)


    println(s"Consumer subscribed to topic $topic")

    val testKV = java.util.UUID.randomUUID().toString
    def waitMessage(failCount: Int = 0): Try[Unit] = {
      val messages = consumer.getMessages
      val matches = messages.filter(msg => msg._1 == testKV && msg._2 == testKV)
      if(failCount >= MAX_INIT_MESSAGE_ATTEMPTS) {
        Failure[Unit](new RuntimeException(s"Failed to initialize topic $topic"))
      } else if(matches.size >= 1) {
        Success(Unit)
      } else {
        Thread.sleep(500)
        println(s"No match: $messages")
        waitMessage(failCount + 1)
      }
    }

    println(s"Sending message to $topic")
    val producer = new SingleTopicProducerKakfa[String, String](topic, getProducerConfig)
    def sendMessage: Future[Unit] = {
      producer
        .send(testKV, testKV)
        .map(_ => println(s"Sent message $testKV, $testKV to $topic"))
        .flatMap(_ => Future { blocking { waitMessage() }})
        .flatMap{result => result match {
            // Retry on failure
          case Failure(e) => sendMessage
          case _ => {
            println(s"Got message on topci $topic")
            Future.successful(Unit)
          }
        }}
    }
    sendMessage
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
