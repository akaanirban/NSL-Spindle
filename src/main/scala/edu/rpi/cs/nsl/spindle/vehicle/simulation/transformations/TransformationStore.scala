package edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations

import edu.rpi.cs.nsl.spindle.vehicle.Types.NodeId
import edu.rpi.cs.nsl.spindle.vehicle.Types.Timestamp
import edu.rpi.cs.nsl.spindle.vehicle.kafka.ClientFactory
import edu.rpi.cs.nsl.spindle.vehicle.kafka.streams.StreamKVReducer
import edu.rpi.cs.nsl.spindle.vehicle.kafka.streams.StreamMapper

abstract class TransformationFunc(funcId: String, inTopic: String, outTopic: String)

case class KvReducerFunc[K >: Null, V >: Null](funcId: String, inTopic: String, outTopic: String, reduceFunc: (V, V) => V)
    extends TransformationFunc(funcId, inTopic, outTopic) {
  def getStreamReducer(clientFactory: ClientFactory): StreamKVReducer[K, V] = {
    clientFactory.mkKvReducer[K, V](inTopic, outTopic, reduceFunc, funcId)
  }
}

case class MapperFunc[K, V, K1, V1](funcId: String, inTopic: String, outTopic: String, mapFunc: (K, V) => (K1, V1))
    extends TransformationFunc(funcId, inTopic, outTopic) {
  def getStreamMapper(clientFactory: ClientFactory): StreamMapper[K, V, K1, V1] = {
    clientFactory.mkMapper(inTopic, outTopic, mapFunc, funcId)
  }
}

case class ActiveTransformations(mappers: Iterable[MapperFunc[Any, Any, Any, Any]], reducers: Iterable[KvReducerFunc[Any, Any]])

abstract class TransformationStore(nodeId: NodeId) {
  def getActiveTransformations(timestamp: Timestamp): ActiveTransformations
}

abstract class TransformationStoreFactory {
  def getTransformationStore(nodeId: NodeId): TransformationStore
}