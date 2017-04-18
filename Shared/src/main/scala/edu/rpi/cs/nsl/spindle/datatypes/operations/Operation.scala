package edu.rpi.cs.nsl.spindle.datatypes.operations

import scala.reflect.runtime.universe._

/**
 * Map or Reduce Operation
 *
 */
abstract class Operation[InType: TypeTag, OutType: TypeTag](val uid: String, val operationId: OperationIds.Value) extends Serializable {
  override def equals(other: Any): Boolean = {
    if (other.getClass != this.getClass) {
      false
    }
    val otherOp = other.asInstanceOf[Operation[InType, OutType]]
    otherOp.operationId == this.operationId &&
      otherOp.getType.toString.equals(this.getType.toString)
  }

  override def hashCode: Int = {
    operationId.hashCode | getType.hashCode
  }

  /**
   * Get the generic's types
   *
   */
  def getType: (TypeTag[InType], TypeTag[OutType]) = (typeTag[InType], typeTag[OutType])
}
