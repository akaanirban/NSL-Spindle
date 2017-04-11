package edu.rpi.cs.nsl.spindle.vehicle

/**
  * Created by wrkronmiller on 3/30/17.
  *
  * Entrypoint for on-vehicle softwware
  */
object Main {
  def main(argv: Array[String]): Unit ={
    println("Vehicle started")
    //TODO
  }
}

object StartupManager {
  private def waitZkKafka(zkString: String, kafkaBrokers: String): Unit = {
    //TODO: wait zk, then wait kafka
  }
  def waitLocal: Unit = {
    waitZkKafka(Configuration.Local.zkString, Configuration.Local.kafkaBrokers)
  }
  //TODO: waitCloud
}