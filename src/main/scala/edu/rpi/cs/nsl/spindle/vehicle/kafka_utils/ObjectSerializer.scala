package edu.rpi.cs.nsl.spindle.vehicle.kafka_utils

import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream
import java.io.ByteArrayInputStream
import java.io.ObjectInputStream

object ObjectSerializer {
  /**
   * Convert java object to byte array
   */
  def serialize[T](obj: T): Array[Byte] = {
    val bytesOut = new ByteArrayOutputStream
    val outStream = new ObjectOutputStream(bytesOut)
    outStream.writeObject(obj)
    outStream.flush
    bytesOut.toByteArray
  }

  /**
   * Convert byte array to Java object
   *
   * @note Should be moved to shared codebase as it is identical to Serialization.load in Spindle Spark
   */
  def deserialize[T](data: Array[Byte]): T = {
    val in = new ObjectInputStream(new ByteArrayInputStream(data))
    in.readObject.asInstanceOf[T]
  }
}