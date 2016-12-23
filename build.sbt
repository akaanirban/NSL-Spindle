organization := "net.kronmiller.william"
version := "0.0.1"
scalaVersion := "2.11.8"

name := "Spindle Vehicle"

libraryDependencies += "com.typesafe" % "config" % "1.3.0"
libraryDependencies += "org.apache.kafka" % "kafka-streams" % "0.10.1.0"

// Logging
libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.21"
// Unit Testing Library
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % "test"
// Docker
libraryDependencies += "edu.rpi.cs.nsl.spindle" %% "docker" % "1.2.0" % "test"
