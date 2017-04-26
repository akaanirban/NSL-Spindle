package edu.rpi.cs.nsl.spindle.datatypes.operations

import scala.reflect.runtime.universe._

//TODO: explicit key and value types
case class MapOperation[InType: TypeTag, OutType: TypeTag](override val uid: String = java.util.UUID.randomUUID.toString, f: (InType) => OutType)
  extends Operation[InType, OutType](uid, OperationIds.map)