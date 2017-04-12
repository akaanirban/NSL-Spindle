package edu.rpi.cs.nsl.spindle.vehicle.queries

import edu.rpi.cs.nsl.spindle.datatypes.Vehicle
import edu.rpi.cs.nsl.spindle.datatypes.operations.{MapOperation, ReduceByKeyOperation}
import edu.rpi.cs.nsl.spindle.vehicle.kafka.streams.{StreamKVReducer, StreamMapper, StreamsConfigBuilder}

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
    * @param configBuilder
    * @return
    */
  def mkExecutors(configBuilder: StreamsConfigBuilder): (StreamMapper[Any, Vehicle, MapKey, MapValue], StreamKVReducer[MapKey, MapValue]) = {
    val mapExecutor = StreamMapper.mkMapper(configBuilder, mapOperation)
    val reduceExecutor = StreamKVReducer.mkReducer[MapKey, MapValue](mapOperation.uid, configBuilder, reduceOperation)
    (mapExecutor, reduceExecutor)
  }
}