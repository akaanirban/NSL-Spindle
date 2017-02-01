package edu.rpi.cs.nsl.spindle.vehicle

import scala.reflect.runtime.universe._
/**
 * Wraps a value to prevent type erasure
 */
case class TypedValue[T: TypeTag](value: T) {
  def getTypeString: String = typeTag[T].toString
}

object ReflectionUtils {
  def getTypeString[T: TypeTag]: String = typeTag[T].toString
  def getMatchingTypes(collection: Iterable[TypedValue[Any]], typeString: String): Iterable[TypedValue[Any]] = {
    collection.filter(_.getTypeString == typeString)
  }
}
