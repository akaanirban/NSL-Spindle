package edu.rpi.cs.nsl.spindle.vehicle.queries

import edu.rpi.cs.nsl.spindle.datatypes.Vehicle
import edu.rpi.cs.nsl.spindle.datatypes.operations.{MapOperation, ReduceOperation}
import scala.reflect.runtime.universe.TypeTag

/**
  * Created by wrkronmiller on 4/11/17.
  */
case class Query[MapValueType: TypeTag, ReduceOutput: TypeTag](id: String,
                                          mapOperation: MapOperation[Vehicle, (Any, MapValueType)],
                                          reduceOperation: ReduceOperation[MapValueType, ReduceOutput]) extends Serializable
