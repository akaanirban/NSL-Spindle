package edu.rpi.cs.nsl.spindle.vehicle.simulation

import akka.actor._
import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.WordSpecLike
import akka.testkit.TestActorRef
import scala.concurrent.duration._
import akka.testkit.ImplicitSender
import edu.rpi.cs.nsl.spindle.vehicle.Types.NodeId
import edu.rpi.cs.nsl.spindle.vehicle.simulation.sensors.SensorFactory
import edu.rpi.cs.nsl.spindle.vehicle.simulation.properties.BasicPropertyFactory
import edu.rpi.cs.nsl.spindle.vehicle.kafka.DockerHelper
import edu.rpi.cs.nsl.spindle.vehicle.kafka.ClientFactoryDockerFixtures
import org.slf4j.LoggerFactory

import edu.rpi.cs.nsl.spindle.vehicle.TestingConfiguration.numVehicles
import edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations.TransformationStore
import edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations.TransformationStoreFactory
import edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations.GenerativeStaticTransformationFactory
import edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations.ActiveTransformations
import edu.rpi.cs.nsl.spindle.tags.LoadTest

class EmptyStaticTransformationFactory extends GenerativeStaticTransformationFactory(_ => ActiveTransformations(Set(), Set()))

class WorldActorFixtures()(implicit system: ActorSystem) {
  import ClientFactoryDockerFixtures._
  private val propertyFactory = new BasicPropertyFactory()
  private val clientFactory = getFactory()(system.dispatcher)
  private val transformFactory = new EmptyStaticTransformationFactory
  val world = system.actorOf(World.propsTest(propertyFactory, transformFactory, clientFactory, initOnly = true))
}

class WorldActorSpecDocker extends TestKit(ActorSystem("WorldActorSpec"))
    with ImplicitSender with WordSpecLike with BeforeAndAfterAll {
  private val logger = LoggerFactory.getLogger(this.getClass)
  override def beforeAll {
    super.beforeAll
    ClientFactoryDockerFixtures.waitReady
  }
  override def afterAll {
    DockerHelper.stopCluster
  }
  "A world actor" should {
    "respond to a ping" in new WorldActorFixtures {
      within(50 milliseconds) {
        world ! Ping
        logger.debug("Sent ping")
        expectMsg(Ping)
        logger.debug("Got ping")
      }
    }
    "spawn vehicle actors on receiving init" in new WorldActorFixtures {
      within(30 minutes) {
        world ! World.InitSimulation
        logger.debug("Sent init message")
        expectMsg(World.Starting)
        logger.debug("Got starting message and waiting for ready message")
        expectMsg(World.Ready(numVehicles))
      }
    }
  }
}