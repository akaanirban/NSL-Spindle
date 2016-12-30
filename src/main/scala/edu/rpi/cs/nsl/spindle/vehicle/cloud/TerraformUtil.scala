package edu.rpi.cs.nsl.spindle.vehicle.cloud

import java.io.File
import scala.sys.process._

import scala.sys.process.ProcessLogger
import org.slf4j.LoggerFactory
import org.slf4j.Logger
import scala.concurrent._
import scala.util.Success

abstract class TerraformManager(workdir: String) {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private def logLine(line: String): String = {
    logger.debug(s"Terraform $line")
    line
  }
  private val processLogger = ProcessLogger(logLine, logLine)
  private object Commands {
    private val terraform = "terraform"
    val output = s"$terraform output"
    val destroy = s"$terraform destroy -force"
    val apply = s"$terraform apply"
    val plan = s"$terraform plan"
  }
  private implicit val ec = ExecutionContext.global
  private def execFuture(process: ProcessBuilder): Future[String] = {
    Future {
      blocking {
        process.!!(processLogger)
      }
    }
  }
  private def execCommand(command: String): Future[String] = {
    execFuture(Process(command, new File(workdir)))
  }
  def apply: Future[String] = {
    execCommand(Commands.apply)
  }
  def destroy = {
    execCommand(Commands.destroy)
  }
  def printPlan {
    execCommand(Commands.plan).onComplete {
      case Success(planString) => System.err.println(s"Terraform plan: $planString")
      case _                   => throw new RuntimeException("Terraform plan failed")
    }
  }
  protected def getOutputMap: Future[Map[String, String]] = {
    execCommand(Commands.output).map {
      _
        .split("\n")
        .map(_.trim)
        .map(_.split(" = "))
        .map(a => (a(0) -> a(1)))
        .toMap
    }
  }
}