package edu.rpi.cs.nsl.spindle.datatypes.operations

import scala.reflect.runtime.universe._

case class ReduceOperation[InType: TypeTag, OutType: TypeTag](f: (InType, InType) => OutType, override val operationId: OperationIds.Value)
  extends Operation[InType, OutType](operationId)

case class ReduceByKeyOperation[InType: TypeTag, OutType: TypeTag](f: (InType, InType) => OutType, override val operationId: OperationIds.Value)
  extends Operation[InType, OutType](operationId)
