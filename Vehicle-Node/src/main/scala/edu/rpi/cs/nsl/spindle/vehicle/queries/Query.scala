package edu.rpi.cs.nsl.spindle.vehicle.queries

import edu.rpi.cs.nsl.spindle.datatypes.Vehicle
import edu.rpi.cs.nsl.spindle.datatypes.operations.{MapOperation, ReduceByKeyOperation}
import edu.rpi.cs.nsl.spindle.vehicle.kafka.executors.{KVReducer, Mapper}

import scala.concurrent.ExecutionContext
import scala.reflect.runtime.universe.TypeTag

/**
  * Created by wrkronmiller on 4/11/17.
  *
  * @todo - add MapKeyType of map() output
  */
case class Query[MapKey: TypeTag, MapValue: TypeTag](id: String,
                                          mapOperation: MapOperation[(Any, Vehicle), (MapKey, MapValue)],
                                          reduceOperation: ReduceByKeyOperation[MapValue]) extends Serializable {
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