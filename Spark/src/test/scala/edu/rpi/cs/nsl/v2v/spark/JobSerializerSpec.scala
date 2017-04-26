package edu.rpi.cs.nsl.v2v.spark

import org.I0Itec.zkclient.ZkClient
import edu.rpi.cs.nsl.v2v.configuration.Configuration
import org.apache.zookeeper.WatchedEvent
import org.slf4j.LoggerFactory
import org.scalatest.BeforeAndAfterAll
import edu.rpi.cs.nsl.spindle.ZKHelper
import edu.rpi.cs.nsl.spindle.datatypes.Vehicle
import edu.rpi.cs.nsl.spindle.datatypes.operations.OperationIds.sum
import edu.rpi.cs.nsl.spindle.datatypes.operations.MapOperation
import edu.rpi.cs.nsl.spindle.datatypes.VehicleTypes._
import edu.rpi.cs.nsl.spindle.datatypes.operations.ReduceOperation
import edu.rpi.cs.nsl.spindle.datatypes.operations.Operation

class JobSerializerSpec extends NSLSpec with BeforeAndAfterAll {
  private val logger = LoggerFactory.getLogger(this.getClass)

  protected val zkTestRoot = "/zkTest"
  protected def mkPath(rel: String): String = s"$zkTestRoot/$rel"

  protected def getUUIDString: String = java.util.UUID.randomUUID.toString

  override def beforeAll {
    this.startContainers
  }
  trait ZKConnected {
    import edu.rpi.cs.nsl.spindle.Configuration.Zookeeper.sessionTimeoutMS
    val zkClient = new ZkClient(getZkConnection)
    def randomPath = mkPath(getUUIDString)
    // Create test root
    zkClient.waitUntilConnected
    if (zkClient.exists(zkTestRoot) == false) {
      zkClient.createPersistent(zkTestRoot)
    }
  }

  "ZKClient" should "successfully connect to zookeeper" in new ZKConnected {
    zkClient.waitUntilConnected
    zkClient.close
  }

  it should "successfully create and destroy a permanent node" in new ZKConnected {
    // Make a random path
    val testPath = randomPath
    zkClient.createPersistent(testPath)
    zkClient.getCreationTime(testPath)
    zkClient.delete(testPath)
    zkClient.close
  }

  it should "successfully create and destroy an ephemeral node" in new ZKConnected {
    val testPath = randomPath
    zkClient.createEphemeral(testPath)
    assert(zkClient.exists(testPath))
    zkClient.delete(testPath)
    zkClient.close
  }

  trait ZKHelperConnected {
    val randomTopic: String = getUUIDString
    val zkHelper = new ZKHelper(getZkConnection, randomTopic)
  }

  "ZKHelper" should "register a test topic" in new ZKHelperConnected {
    zkHelper.registerTopic
    zkHelper.close
  }

  it should "register a test query" in new ZKHelperConnected {
    zkHelper.registerTopic
    val query = {
      val mapOp = MapOperation[Vehicle, MPH](_.mph)
      val reduceOp = ReduceOperation[MPH, Double](_ + _, sum)
      Seq(mapOp, reduceOp)
    }
    zkHelper.registerQuery(query)

    try {
      assert(zkHelper.zkClient.exists(zkHelper.queryPath))
      val loadedQuery = zkHelper.zkClient.readData[Seq[Operation[_, _]]](zkHelper.queryPath)
      0.until(loadedQuery.size).map { index =>
        val loadedOp = loadedQuery(index)
        val savedOp = query(index)
        def describeStr(op: Operation[_, _]): String = {
          s"$op: [${op.getType._1}, ${op.getType._2}]"
        }
        assert(loadedOp.equals(savedOp), s"TypeMatch: ${describeStr(loadedOp)} != ${describeStr(savedOp)}")
      }
    } finally {
      zkHelper.close
    }
  }

  override def afterAll {
    this.stopContainers
  }
}