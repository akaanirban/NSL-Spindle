package edu.rpi.cs.nsl.spindle.vehicle.queries

import edu.rpi.cs.nsl.spindle.datatypes.Vehicle
import edu.rpi.cs.nsl.spindle.datatypes.operations.{MapOperation, ReduceByKeyOperation}

import scala.reflect.runtime.universe.TypeTag

/**
  * Created by wrkronmiller on 4/11/17.
  *
  * @todo - add MapKeyType of map() output
  */
case class Query[MapKey: TypeTag, MapValue: TypeTag](id: String,
                                          mapOperation: MapOperation[(Any, Vehicle), (MapKey, MapValue)],
                                          reduceOperation: ReduceByKeyOperation[MapValue]) extends Serializable