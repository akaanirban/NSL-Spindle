package edu.rpi.cs.nsl.spindle.vehicle.kafka

import edu.rpi.cs.nsl.spindle.datatypes.Vehicle
import edu.rpi.cs.nsl.spindle.vehicle.kafka.executors.{KVReducer, Mapper}
import edu.rpi.cs.nsl.spindle.vehicle.queries.Query

import scala.reflect.runtime.universe.TypeTag

import scala.concurrent.ExecutionContext

/**
  * Created by wrkronmiller on 5/3/17.
  */
object KafkaQueryUtils {
  implicit class KafkaQuery[MapKey: TypeTag, MapValue: TypeTag](query: Query[MapKey, MapValue]) {
    import query._
    /**
      * Create Kafka Streams executors from query
      * @return
      */
    def mkExecutors(implicit ec: ExecutionContext): (Mapper[Any, Vehicle, MapKey, MapValue], KVReducer[MapKey, MapValue]) = {
      val mapExecutor = Mapper.mkSensorMapper[MapKey, MapValue](mapOperation.uid, mapOperation.f, (mapKey, mapVal) => mapOperation.filter((mapKey, mapVal)))
      val reduceExecutor = KVReducer.mkVehicleReducer[MapKey, MapValue](reduceOperation.uid, mapOperation.uid, reduceOperation.f)
      (mapExecutor, reduceExecutor)
    }
  }
}