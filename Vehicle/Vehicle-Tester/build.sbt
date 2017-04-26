organization := "net.kronmiller.william"
version := "0.2.0"
scalaVersion := "2.11.8"

name := "VehicleSimulator"

libraryDependencies += "com.typesafe" % "config" % "1.3.0"

// https://mvnrepository.com/artifact/org.scala-lang/scala-reflect
libraryDependencies += "org.scala-lang" % "scala-reflect" % "2.11.8"

// Unit testing
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % "test"

// Logging
libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.21"
// Postgres
libraryDependencies += "org.postgresql" % "postgresql" % "9.4.1212.jre7"

//libraryDependencies += "edu.rpi.cs.nsl.spindle" %% "shared-lib" % "1.4.0"


lazy val sharedLib = RootProject(file("../../Shared"))
lazy val vehicleNode = RootProject(file("../Vehicle-Node"))
val main = Project(id = "NSL-Spark", base = file("."))
    .dependsOn(sharedLib)

// Enable scala experimental compiler flags
scalacOptions ++= Seq("-Xexperimental", "-feature", "-deprecation", "-language:postfixOps")


// Enable parallel testing
parallelExecution in Test := false


def cloudFilter(name: String): Boolean = name endsWith "Cloud"
def unitFilter(name: String): Boolean = cloudFilter(name) == false
