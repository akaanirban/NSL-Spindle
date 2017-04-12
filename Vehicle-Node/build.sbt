organization := "net.kronmiller.william"
version := "0.0.1"
scalaVersion := "2.11.8"

name := "SpindleVehicle"

libraryDependencies += "com.typesafe" % "config" % "1.3.0"
libraryDependencies += "org.apache.kafka" % "kafka-streams" % "0.10.2.0"
libraryDependencies += "org.apache.kafka" %% "kafka" % "0.10.2.0"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.10.2.0"

// https://mvnrepository.com/artifact/org.scala-lang/scala-reflect
libraryDependencies += "org.scala-lang" % "scala-reflect" % "2.11.8"

// Unit testing
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % "test"

// Logging
libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.21"
// Unit Testing Library
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0"

lazy val sharedLib = RootProject(file("../../Shared"))

val main = Project(id = "NSL-Spark", base = file("."))
    .dependsOn(sharedLib)
// Enable scala experimental compiler flags
scalacOptions ++= Seq("-Xexperimental", "-feature", "-deprecation", "-language:postfixOps")

// Enable parallel testing
parallelExecution in Test := false
