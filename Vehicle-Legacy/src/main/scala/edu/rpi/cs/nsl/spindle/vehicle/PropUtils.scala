package edu.rpi.cs.nsl.spindle.vehicle

import java.util.Properties

object PropUtils {
  implicit class PipelinedProps(properties: Properties) {
    def withPut(key: Object, value: Object): Properties = {
      properties.put(key, value)
      properties
    }
  }
}
