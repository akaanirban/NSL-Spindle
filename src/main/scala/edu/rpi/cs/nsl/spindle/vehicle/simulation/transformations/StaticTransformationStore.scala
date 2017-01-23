package edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations

import edu.rpi.cs.nsl.spindle.vehicle.Types.NodeId
import edu.rpi.cs.nsl.spindle.vehicle.Types.Timestamp

/**
 * Always return the same set of transformation operations
 */
class StaticTransformationStore(nodeId: NodeId, transformations: ActiveTransformations)
    extends TransformationStore(nodeId) {
  def getActiveTransformations(timestamp: Timestamp): ActiveTransformations = transformations
}

abstract class StaticTransformationFactory
    extends TransformationStoreFactory {
  protected def getActiveTransformations(nodeId: NodeId): ActiveTransformations
  def getTransformationStore(nodeId: NodeId): TransformationStore = {
    new StaticTransformationStore(nodeId, getActiveTransformations(nodeId))
  }
}

class MappedStaticTransformationFactory(transformationMap: Map[NodeId, ActiveTransformations])
    extends StaticTransformationFactory {
  protected def getActiveTransformations(nodeId: NodeId): ActiveTransformations = {
    transformationMap(nodeId)
  }
}

class GenerativeStaticTransformationFactory(generator: (NodeId) => ActiveTransformations)
    extends StaticTransformationFactory {
  protected def getActiveTransformations(nodeId: NodeId) = generator(nodeId)
}