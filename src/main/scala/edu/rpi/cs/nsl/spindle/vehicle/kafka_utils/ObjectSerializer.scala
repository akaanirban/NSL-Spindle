package edu.rpi.cs.nsl.spindle.vehicle.kafka_utils

import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream

object ObjectSerializer {
  def serialize[T](obj: T): Array[Byte] = {
    val bytesOut = new ByteArrayOutputStream
    val outStream = new ObjectOutputStream(bytesOut)
    outStream.writeObject(obj)
    outStream.flush
    bytesOut.toByteArray
  }
}