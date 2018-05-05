package edu.rpi.cs.nsl.spindle.vehicle.kafka

import edu.rpi.cs.nsl.spindle.datatypes.Vehicle
import edu.rpi.cs.nsl.spindle.vehicle.kafka.executors.{GossipReducer, KVReducer, Mapper}
import edu.rpi.cs.nsl.spindle.vehicle.queries.Query

import scala.reflect.runtime.universe.TypeTag
import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag

/**
  * Created by wrkronmiller on 5/3/17.
  */
object KafkaQueryUtils {
  implicit class KafkaQuery[MapKey: TypeTag: ClassTag, MapValue: TypeTag: ClassTag](query: Query[MapKey, MapValue]) {
    import query._
    /**
      * Create Kafka Streams executors from query
      * @return
      */
    def mkExecutors(implicit ec: ExecutionContext): (Mapper[Any, Vehicle, MapKey, MapValue], KVReducer[MapKey, MapValue]) = {
      val mapExecutor = Mapper.mkSensorMapper[MapKey, MapValue](mapperId = mapOperation.uid, queryUid = query.id, mapOperation.f, (mapKey, mapVal) => mapOperation.filter((mapKey, mapVal)))
//      val reduceExecutor = KVReducer.mkVehicleReducer[MapKey, MapValue](reducerId = reduceOperation.uid, queryUid = id, mapperId = mapOperation.uid, reduceOperation.f)
      val reduceExecutor = GossipReducer.mkVehicleReducer[MapKey, MapValue](reducerId = reduceOperation.uid, queryUid = id, mapperId = mapOperation.uid, reduceOperation.f)
      (mapExecutor, reduceExecutor)
    }
  }
}
