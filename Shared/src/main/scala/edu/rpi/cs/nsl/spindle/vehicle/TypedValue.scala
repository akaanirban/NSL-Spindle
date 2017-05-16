package edu.rpi.cs.nsl.spindle.vehicle

import scala.reflect.{classTag, ClassTag}
import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
 * Wraps a value to prevent type erasure
 */
case class TypedValue[T: TypeTag: ClassTag](value: T,
                                            queryUid: Option[String] = None,
                                            creationEpoch: Long = System.currentTimeMillis(),
                                            isCanary: Boolean = false) {
}

object ReflectionUtils {
  def getTypeString[T: TypeTag]: String = typeTag[T].toString
  def getClassString[T: ClassTag]: String = classTag[T].toString
  def getClassString[T: ClassTag](elem: T): String = getClassString[T]
}
