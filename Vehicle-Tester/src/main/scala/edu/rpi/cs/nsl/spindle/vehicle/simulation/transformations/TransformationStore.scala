package edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations

import edu.rpi.cs.nsl.spindle.datatypes.VehicleTypes.{Lat, Lon}
import edu.rpi.cs.nsl.spindle.vehicle.Types.NodeId
import edu.rpi.cs.nsl.spindle.vehicle.Types.Timestamp
import edu.rpi.cs.nsl.spindle.vehicle.kafka.ClientFactory
import edu.rpi.cs.nsl.spindle.vehicle.kafka.streams.StreamKVReducer
import edu.rpi.cs.nsl.spindle.vehicle.kafka.streams.StreamMapper
import edu.rpi.cs.nsl.spindle.vehicle.kafka.streams.StreamExecutor
import org.slf4j.LoggerFactory

import scala.reflect.runtime.universe._

abstract class TransformationFunc(val funcId: String, inTopic: String, outTopic: String) {
  override def hashCode: Int = funcId.hashCode
  override def equals(obj: Any): Boolean = {
    //scalastyle:off null
    if (obj == null) {
      //scalastyle:on null
      false
    } else if (obj.isInstanceOf[TransformationFunc] == false) {
      false
    } else {
      obj.asInstanceOf[TransformationFunc].funcId == funcId
    }
  }
  override def toString: String = {
    s"${super.toString}_funcId-$funcId-topics:-$inTopic->$outTopic"
  }
  def getTransformExecutor(clientFactory: ClientFactory): StreamExecutor
}

case class KvReducerFunc[K: TypeTag, V: TypeTag](override val funcId: String,
                                                 inTopic: String,
                                                 outTopic: String,
                                                 reduceFunc: (V, V) => V)
    extends TransformationFunc(funcId, inTopic, outTopic) {
  private val logger = LoggerFactory.getLogger(this.getClass)
  def getStreamReducer(clientFactory: ClientFactory): StreamKVReducer[K, V] = {
    logger.info(s"Creating kv reducer $funcId topics: $inTopic->$outTopic")
    clientFactory.mkKvReducer[K, V](inTopic = inTopic, outTopic, reduceFunc, funcId)
  }
  def getTransformExecutor(clientFactory: ClientFactory): StreamExecutor = getStreamReducer(clientFactory)
}

case class MapperFunc[K: TypeTag, V: TypeTag, K1: TypeTag, V1: TypeTag](override val funcId: String,
                                                                        inTopic: String,
                                                                        outTopic: String,
                                                                        mapFunc: (K, V) => (K1, V1),
                                                                        filterFunc: (K,V) => Boolean = (_: K ,_: V) => true)
    extends TransformationFunc(funcId, inTopic, outTopic) {
  def getStreamMapper(clientFactory: ClientFactory): StreamMapper[K, V, K1, V1] = {
    clientFactory.mkMapper(inTopic, outTopic, mapFunc, filterFunc, funcId)
  }
  def getTransformExecutor(clientFactory: ClientFactory): StreamExecutor = getStreamMapper(clientFactory)
}

case class ActiveTransformations(mappers: Iterable[MapperFunc[_, _, _, _]], reducers: Iterable[KvReducerFunc[_, _]])
abstract class TransformationStore(nodeId: NodeId) {
  def getActiveTransformations(timestamp: Timestamp, position: (Lat, Lon)): ActiveTransformations
}

abstract class TransformationStoreFactory {
  def getTransformationStore(nodeId: NodeId): TransformationStore
}