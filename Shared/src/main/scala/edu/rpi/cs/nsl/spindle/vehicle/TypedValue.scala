package edu.rpi.cs.nsl.spindle.vehicle

import scala.reflect.{classTag, ClassTag}
import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
 * Wraps a value to prevent type erasure
 */
case class TypedValue[T: TypeTag: ClassTag](value: T, creationEpoch: Long = System.currentTimeMillis(), isCanary: Boolean = false) {
  @transient private lazy val typeString = typeTag[T].toString
  private lazy val classString= classTag[T].toString
  def getTypeString: String = typeString
  def getClassString: String = classString
  def getType = typeTag[T].tpe
}

object ReflectionUtils {
  def getTypeString[T: TypeTag]: String = typeTag[T].toString
  def getMatchingTypes(collection: Iterable[TypedValue[Any]], typeString: String): Iterable[TypedValue[Any]] = {
    collection.filter(_.getTypeString == typeString)
  }
  def getClassString[T: ClassTag]: String = classTag[T].toString
  def getClassString[T: ClassTag](elem: T): String = getClassString[T]
}
