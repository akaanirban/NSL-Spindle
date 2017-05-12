package edu.rpi.cs.nsl.spindle.vehicle.kafka.utils

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.slf4j.LoggerFactory

import scala.language.implicitConversions
import scala.reflect.{classTag, ClassTag}
import scala.reflect.runtime.universe.TypeTag

object ObjectSerializer {
  /**
   * Convert java object to byte array
   */
  def serialize[T: TypeTag: ClassTag](obj: T): Array[Byte] = {
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
  def deserialize[T: TypeTag: ClassTag](data: Array[Byte]): T = {
    val in = new ObjectInputStream(new ByteArrayInputStream(data))
    val tObject = in.readUnshared()
    val tCast = tObject.asInstanceOf[T]
    in.close
    tCast
  }
}

trait SerDeNop {
  def close {}
  def configure(map: java.util.Map[String, _], bool: Boolean) {}
}

class KafkaSerializer[T: TypeTag: ClassTag] extends Serializer[T] with SerDeNop {
  private val logger = LoggerFactory.getLogger(this.getClass)
  def serialize(topic: String, t: T): Array[Byte] = {
    ObjectSerializer.serialize(t)
  }
}

//scalastyle:off null
class KafkaDeserializer[T >: Null: TypeTag: ClassTag] extends Deserializer[T] with SerDeNop {
  def deserialize(topic: String, data: Array[Byte]): T = {
    if (data == null) {
      null
    } else {
      ObjectSerializer.deserialize[T](data)
    }
  }
}

class KafkaSerde[T >: Null: TypeTag: ClassTag] extends Serde[T] with SerDeNop {
  def deserializer: KafkaDeserializer[T] = new KafkaDeserializer[T]()
  def serializer: KafkaSerializer[T] = new KafkaSerializer[T]()
}
//scalastyle:on null