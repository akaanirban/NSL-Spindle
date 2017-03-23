organization := "net.kronmiller.william"
version := "0.0.1"
scalaVersion := "2.11.8"

name := "Spindle Middleware"

libraryDependencies += "com.typesafe" % "config" % "1.3.0"
libraryDependencies += "org.apache.kafka" % "kafka-streams" % "0.10.1.1"
libraryDependencies += "org.apache.kafka" %% "kafka" % "0.10.1.1"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.10.1.1"

libraryDependencies += "org.scala-lang" % "scala-reflect" % "2.11.8"

// Unit testing
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % "test"

// Logging
libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.21"
// Unit Testing Library
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0"
// Postgres
libraryDependencies += "org.postgresql" % "postgresql" % "9.4.1212.jre7"
// Scalaz Extensions
libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.2.8"
// Akka
libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.4.16"
libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % "2.4.16"
libraryDependencies += "com.typesafe.akka" %% "akka-remote" % "2.4.16"


lazy val sharedLib = RootProject(file("../Shared"))
val main = Project(id = "NSL-Spark", base = file("."))
    .dependsOn(sharedLib)

// Enable scala experimental compiler flags
scalacOptions ++= Seq("-Xexperimental", "-feature", "-deprecation", "-language:postfixOps")

// Enable parallel testing
parallelExecution in Test := true
