organization := "net.kronmiller.william"
version := "0.0.1"
scalaVersion := "2.11.8"

name := "Spindle Vehicle"

libraryDependencies += "com.typesafe" % "config" % "1.3.0"
libraryDependencies += "org.apache.kafka" % "kafka-streams" % "0.10.1.0"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.10.1.0"

// https://mvnrepository.com/artifact/org.scala-lang/scala-reflect
libraryDependencies += "org.scala-lang" % "scala-reflect" % "2.11.8"

// Logging
libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.21"
// Unit Testing Library
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % "test"
// Docker
libraryDependencies += "edu.rpi.cs.nsl.spindle" %% "shared-lib" % "1.3.0"

// Disable parallel testing
parallelExecution in Test := false
