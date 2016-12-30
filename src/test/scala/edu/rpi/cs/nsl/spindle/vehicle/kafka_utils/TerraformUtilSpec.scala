package edu.rpi.cs.nsl.spindle.vehicle.kafka_utils

import scala.concurrent.Await
import scala.concurrent.duration._

import org.scalatest.BeforeAndAfterAll
import org.scalatest.FlatSpec
import org.slf4j.LoggerFactory
import java.net.InetAddress
import org.scalatest.DoNotDiscover

@DoNotDiscover
object KafkaTestFactory {
  import Constants._
  def mkTester(serverList: ServerList): KafkaSharedTests = {
    val kafkaAdmin = new KafkaAdmin(s"${serverList.zookeeper}:2181")
    val kafkaConfig: KafkaConfig = {
      val servers = serverList.brokers.map(a => s"$a:$KAFKA_DEFAULT_PORT").mkString(",")
      KafkaConfig().withServers(servers)
    }
    new KafkaSharedTests(kafkaConfig, kafkaAdmin)
  }
}

class TerraformUtilSpecCloud extends FlatSpec with BeforeAndAfterAll {
  private val logger = LoggerFactory.getLogger(this.getClass)
  val CLOUD_WAIT_TIME = 10 minutes

  override def beforeAll {
    logger.info("Destroying kafka cluster if one exists")
    Await.result(TerraformUtils.destroy, CLOUD_WAIT_TIME)
    TerraformUtils.printPlan
    logger.info("Running terraform apply")
    Await.result(TerraformUtils.apply, CLOUD_WAIT_TIME)
  }

  it should "get the server list" in {
    val serverList = Await.result(TerraformUtils.getServers, 1 minutes)
    logger.debug(s"Got servers $serverList")
    val zkInet = InetAddress.getByName(serverList.zookeeper)
  }

  //TODO: break out into suite
  private lazy val sharedKafkaTests = KafkaTestFactory.mkTester(Await.result(TerraformUtils.getServers, 1 minutes))

  "Kafka" should "not lose any messages" in {
    sharedKafkaTests.testSendRecv
  }

  override def afterAll {
    logger.info("Destroying kafka cluster")
    Await.result(TerraformUtils.destroy, CLOUD_WAIT_TIME)
  }
}