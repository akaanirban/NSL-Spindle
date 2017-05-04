package edu.rpi.cs.nsl.spindle.datatypes.operations

import scala.reflect.runtime.universe._

//TODO: explicit key and value types
case class MapOperation[InType: TypeTag, OutType: TypeTag](f: (InType) => OutType, override val uid: String = java.util.UUID.randomUUID.toString, filter: (InType) => Boolean = (_: InType) => true)
  extends Operation[InType, OutType](uid, OperationIds.map)