package edu.rpi.cs.nsl.spindle.datatypes.operations

import scala.reflect.runtime.universe._


case class MapOperation[InType: TypeTag, OutType: TypeTag](f: (InType) => OutType)
  extends Operation[InType, OutType](OperationIds.map)
