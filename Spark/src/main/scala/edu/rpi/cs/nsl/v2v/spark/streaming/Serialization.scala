package edu.rpi.cs.nsl.v2v.spark.streaming

import kafka.serializer.Decoder
import java.io.ObjectOutputStream
import java.io.ByteArrayOutputStream
import java.io.ByteArrayInputStream
import java.io.ObjectInputStream

/**
 * Kafka Stream Serialization Utils/Classes
 */
object Serialization {
  trait KafkaMessageComponent extends Serializable {
    def dump: Array[Byte] = {
      val bytes = new ByteArrayOutputStream()
      val out = new ObjectOutputStream(bytes)
      out.writeObject(this)
      out.close
      bytes.toByteArray
    }
  }

  def load[T](data: Array[Byte]): T = {
    val in = new ObjectInputStream(new ByteArrayInputStream(data))
    in.readObject.asInstanceOf[T]
  }

  /**
   * @param uuid - some unique identifier (probably unnecessary)
   * @param oplogIndex - opLog index of the operation applied to the tuple
   */
  final case class KafkaKey(uuid: String, oplogIndex: Int) extends KafkaMessageComponent
  final case class MiddlewareResults(value: Any) extends KafkaMessageComponent

  /**
   * Decode KafkaKey
   */
  class KeyDecoder extends Decoder[KafkaKey] {
    def fromBytes(bytes: Array[Byte]): KafkaKey = {
      load[KafkaKey](bytes)
    }
  }

  /**
   * Decode MiddlewareResult class
   */
  class ValueDecoder extends Decoder[MiddlewareResults] {
    def fromBytes(bytes: Array[Byte]): MiddlewareResults = {
      load[MiddlewareResults](bytes)
    }
  }
}
