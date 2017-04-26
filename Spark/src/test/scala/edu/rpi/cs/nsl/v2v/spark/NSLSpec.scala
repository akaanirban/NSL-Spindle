package edu.rpi.cs.nsl.v2v.spark

import org.scalatest.BeforeAndAfter
import org.scalatest.GivenWhenThen
import org.scalatest.FlatSpec
import org.scalatest.BeforeAndAfterAll
import edu.rpi.cs.nsl.spindle.KafkaZookeeperFixture

/**
 * NSL Spark Test Specification
 */
trait NSLSpec extends FlatSpec with BeforeAndAfter with GivenWhenThen with KafkaZookeeperFixture
