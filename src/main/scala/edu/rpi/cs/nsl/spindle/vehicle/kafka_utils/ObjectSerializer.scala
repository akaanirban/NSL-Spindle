package edu.rpi.cs.nsl.spindle.vehicle.kafka_utils

import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream
import java.io.ByteArrayInputStream
import java.io.ObjectInputStream
import org.apache.kafka.common.serialization.Serializer
import org.slf4j.LoggerFactory
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde

object ObjectSerializer {
  /**
   * Convert java object to byte array
   */
  def serialize[T](obj: T): Array[Byte] = {
    val bytesOut = new ByteArrayOutputStream
    val outStream = new ObjectOutputStream(bytesOut)
    outStream.writeObject(obj)
    outStream.flush
    outStream.close
    bytesOut.toByteArray
  }

  /**
   * Convert byte array to Java object
   *
   * @note Should be moved to shared codebase as it is identical to Serialization.load in Spindle Spark
   */
  def deserialize[T](data: Array[Byte]): T = {
      val in = new ObjectInputStream(new ByteArrayInputStream(data))
      val t = in.readObject.asInstanceOf[T]
      in.close
      t
  }
}

trait SerDeNop {
  def close {}
  def configure(map: java.util.Map[String, _], bool: Boolean) {}
}

class KafkaSerializer[T] extends Serializer[T] with SerDeNop {
  private val logger = LoggerFactory.getLogger(this.getClass)
  def serialize(topic: String, t: T): Array[Byte] = {
    ObjectSerializer.serialize(t)
  }
}

class KafkaDeserializer[T >: Null] extends Deserializer[T] with SerDeNop {
  def deserialize(topic: String, data: Array[Byte]): T = {
    if(data == null){
      null
    } else {
      ObjectSerializer.deserialize[T](data)
    }
  }
}

class KafkaSerde[T >: Null] extends Serde[T] with SerDeNop {
  def deserializer = new KafkaDeserializer[T]()
  def serializer = new KafkaSerializer[T]()
}