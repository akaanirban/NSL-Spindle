package edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations

import edu.rpi.cs.nsl.spindle.vehicle.Types.NodeId
import edu.rpi.cs.nsl.spindle.vehicle.Types.Timestamp
import edu.rpi.cs.nsl.spindle.vehicle.kafka.ClientFactory
import edu.rpi.cs.nsl.spindle.vehicle.kafka.streams.StreamKVReducer
import edu.rpi.cs.nsl.spindle.vehicle.kafka.streams.StreamMapper
import edu.rpi.cs.nsl.spindle.vehicle.kafka.streams.TypedStreamExecutor
import edu.rpi.cs.nsl.spindle.vehicle.kafka.streams.StreamExecutor

abstract class TransformationFunc(val funcId: String, inTopic: String, outTopic: String) {
  override def hashCode: Int = funcId.hashCode
  override def equals(obj: Any) = {
    if (obj == null) {
      false
    } else if (obj.isInstanceOf[TransformationFunc] == false) {
      false
    } else {
      obj.asInstanceOf[TransformationFunc].funcId == funcId
    }
  }
  def getTransformExecutor(clientFactory: ClientFactory): StreamExecutor
}

case class KvReducerFunc[K >: Null, V >: Null](override val funcId: String, inTopic: String, outTopic: String, reduceFunc: (V, V) => V)
    extends TransformationFunc(funcId, inTopic, outTopic) {
  def getStreamReducer(clientFactory: ClientFactory): StreamKVReducer[K, V] = {
    clientFactory.mkKvReducer[K, V](inTopic, outTopic, reduceFunc, funcId)
  }
  def getTransformExecutor(clientFactory: ClientFactory) = getStreamReducer(clientFactory)
}

case class MapperFunc[K, V, K1, V1](override val funcId: String, inTopic: String, outTopic: String, mapFunc: (K, V) => (K1, V1))
    extends TransformationFunc(funcId, inTopic, outTopic) {
  def getStreamMapper(clientFactory: ClientFactory): StreamMapper[K, V, K1, V1] = {
    clientFactory.mkMapper(inTopic, outTopic, mapFunc, funcId)
  }
  def getTransformExecutor(clientFactory: ClientFactory) = getStreamMapper(clientFactory)
}

case class ActiveTransformations(mappers: Iterable[MapperFunc[Any, Any, Any, Any]], reducers: Iterable[KvReducerFunc[Any, Any]])

abstract class TransformationStore(nodeId: NodeId) {
  def getActiveTransformations(timestamp: Timestamp): ActiveTransformations
}

abstract class TransformationStoreFactory {
  def getTransformationStore(nodeId: NodeId): TransformationStore
}