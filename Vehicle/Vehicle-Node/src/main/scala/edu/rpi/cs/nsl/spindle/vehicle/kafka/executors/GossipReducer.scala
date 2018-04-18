package edu.rpi.cs.nsl.spindle.vehicle.kafka.executors

import edu.rpi.cs.nsl.spindle.vehicle.Configuration
import edu.rpi.cs.nsl.spindle.vehicle.gossip.GossipRunner
import edu.rpi.cs.nsl.spindle.vehicle.gossip.results.GossipResultParser
import edu.rpi.cs.nsl.spindle.vehicle.kafka.utils.TopicLookupService
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Duration, MILLISECONDS}
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import scala.collection.JavaConverters._;

/**
  * Perform ReduceByKey on Kafka messages
  *
  * @param uid
  * @param sourceTopics
  * @param sinkTopics
  * @param reduceFunc
  * @tparam K
  * @tparam V
  */
class GossipReducer[K:TypeTag: ClassTag, V:TypeTag: ClassTag](uid: String,
                                                          queryUid: String,
                                                          sourceTopics: Set[GlobalTopic],
                                                          sinkTopics: Set[GlobalTopic],
                                                          reduceFunc: (V,V) => V)(implicit ec: ExecutionContext)
  extends KVReducer[K,V](uid, queryUid, sourceTopics, sinkTopics, reduceFunc) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val thisId = System.getenv("NODE_ID")
  private val numNodes = System.getenv("NUM_NODES")
  logger.debug("going to start gossip!")
  GossipRunner.TryStart(thisId, numNodes)
  logger.debug("trying to get result")
  private val gossipResult = GossipRunner.GetInstance().GetResult()
  logger.debug("done getting result")
  private val gossipResultParser = new GossipResultParser[K, V](gossipResult)
  logger.debug("done getting result parser")


  // Producer inputs and outputs should be tagged
  override def getConsumerQueryUid: Option[String] = Some(queryUid)
  override def getProducerQueryUid: Option[String] = Some(queryUid)

  /**
    * Perform executor-specific transformations
    *
    * @param messages - input messages
    * @return output messages
    */
  override protected def doTransforms(messages: Iterable[(K, V)]): Iterable[(K, V)] = {
//    messages
//      .groupBy(_._1)
//      .mapValues(_.map(_._2))
//      .mapValues{values =>
//        values.reduce(reduceFunc)
//      }
//      .toSeq
//    logger.debug("doing right transform, data is: {}", messages)
//    val result = gossipResultParser.GetResult(queryUid)
//    logger.debug("done getting result from parser")
//    val scalamap = result.asScala
//    logger.debug("done converting to scala obj")
//    val scalaIter = scalamap
//    logger.debug("done doing scala itr, sending: {}", scalamap)
//    scalaIter
    logger.debug("doing good transform! data is {}", messages)
    val result = messages
      .groupBy(_._1)
      .mapValues(_.map(_._2))
      .mapValues{values =>
        values.reduce(reduceFunc)
      }
      .toSeq

    val oneVarReduce = (v:V) => { reduceFunc(v, null.asInstanceOf[V]) }
    val applyReduce = (i:(K, V)) => {(i._1, oneVarReduce(i._2))}
    val fr = result map {applyReduce(_)}

    // will now have list with either 1 or 0 items
    //result.groupBy(_._1).mapValues(_.map(_._2)).mapValues(values => applyReduce(values))
    //val fr = result.map(applyReduce) //map(applyReduce)



    // call the dummy reduce func...
    //val result = Iterable(reduceFunc(null.asInstanceOf[V], null.asInstanceOf[V]))

    logger.debug("done transform, sending {}", fr)
    fr
  }

  override def run(sleepInterval: Duration = Duration(Configuration.Streams.reduceWindowSizeMs, MILLISECONDS)): Unit = {
    logger.debug(s"Reducer $uid will run every $sleepInterval transforming from $sourceTopics to $sinkTopics")
    super.run(sleepInterval)
  }
}

/**
  * Factory for Kafka KV Reducer Executors
  */
object GossipReducer {
  /**
    * Create a KV Reducer for Vehicle Data
    * @param reducerId
    * @param mapperId
    * @param reduceFunc
    * @tparam K
    * @tparam V
    * @return
    */
  def mkVehicleReducer[K: TypeTag: ClassTag,
  V: TypeTag: ClassTag](reducerId: String,
                        queryUid: String,
                        mapperId: String,
                        reduceFunc: (V,V) => V)(implicit ec: ExecutionContext): KVReducer[K,V] = {
    // Reducer reads from clusterhead input
    val sourceTopics = Set(TopicLookupService.getClusterInput).map(GlobalTopic.mkLocalTopic)
    val sinkTopics = Set(TopicLookupService.getReducerOutput).map(GlobalTopic.mkLocalTopic)
    new GossipReducer[K,V](uid=reducerId, queryUid=queryUid, sourceTopics, sinkTopics, reduceFunc)
  }
}
