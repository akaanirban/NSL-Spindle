package edu.rpi.cs.nsl.spindle.vehicle.simulation.transformations

import edu.rpi.cs.nsl.spindle.datatypes.VehicleTypes.{Lat, Lon}
import edu.rpi.cs.nsl.spindle.vehicle.Types.NodeId
import edu.rpi.cs.nsl.spindle.vehicle.Types.Timestamp

/**
 * Always return the same set of transformation operations
 */
class StaticTransformationStore(nodeId: NodeId, transformations: ActiveTransformations)
    extends TransformationStore(nodeId) {
  def getActiveTransformations(timestamp: Timestamp, position: (Lat, Lon)): ActiveTransformations = transformations
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

class EmptyStaticTransformationFactory extends GenerativeStaticTransformationFactory(_ => ActiveTransformations(Set(), Set()))