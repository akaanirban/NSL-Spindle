package edu.rpi.cs.nsl.spindle.vehicle.simulation

import akka.actor._
import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.WordSpecLike
import akka.testkit.TestActorRef
import scala.concurrent.duration._
import akka.testkit.ImplicitSender
import edu.rpi.cs.nsl.spindle.vehicle.simulation.sensors.SensorFactory
import edu.rpi.cs.nsl.spindle.vehicle.simulation.properties.BasicPropertyFactory
import edu.rpi.cs.nsl.spindle.vehicle.kafka.DockerHelper
import edu.rpi.cs.nsl.spindle.vehicle.kafka.ClientFactoryDockerFixtures
import org.slf4j.LoggerFactory

class WorldActorFixtures()(implicit system: ActorSystem) {
  import ClientFactoryDockerFixtures._
  private val propertyFactory = new BasicPropertyFactory()
  private val clientFactory = getFactory
  val world = system.actorOf(World.props(propertyFactory, clientFactory))
}

class WorldActorSpecDocker extends TestKit(ActorSystem("WorldActorSpec")) with ImplicitSender with WordSpecLike with BeforeAndAfterAll {
  private val logger = LoggerFactory.getLogger(this.getClass)
  override def beforeAll {
    super.beforeAll
    ClientFactoryDockerFixtures.waitReady
  }
  "A world actor" should {
    "respond to a ping" in new WorldActorFixtures {
      within(50 milliseconds){
        world ! Ping
        logger.debug("Sent ping")
        expectMsg(Ping)
        logger.debug("Got ping")
      }
    }
    "spawn vehicle actors on receiving init" in new WorldActorFixtures {
       within(2 minutes){
         world ! World.InitSimulation
         logger.debug("Sent init message")
         expectMsg(World.Starting)
         logger.debug("Got starting message and waiting for ready message")
         expectMsg(World.Ready)
       }
    }
  }
}