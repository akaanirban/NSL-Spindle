organization := "net.kronmiller.william"
version := "0.0.1"
scalaVersion := "2.11.8"

name := "Spindle Vehicle"

libraryDependencies += "com.typesafe" % "config" % "1.3.0"
libraryDependencies += "org.apache.kafka" % "kafka-streams" % "0.10.1.1"
libraryDependencies += "org.apache.kafka" %% "kafka" % "0.10.1.1"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.10.1.1"

// https://mvnrepository.com/artifact/org.scala-lang/scala-reflect
libraryDependencies += "org.scala-lang" % "scala-reflect" % "2.11.8"

// Unit testing
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % "test"

// Logging
libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.21"
// Unit Testing Library
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0"
// Postgres
libraryDependencies += "org.postgresql" % "postgresql" % "9.4.1212.jre7"
// Akka
libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.4.16"
libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % "2.4.16"

// Docker
//libraryDependencies += "edu.rpi.cs.nsl.spindle" %% "shared-lib" % "1.4.0"


lazy val sharedLib = RootProject(file("../Shared"))
lazy val CloudTest = config("cloud") extend(Test)
val main = Project(id = "NSL-Spark", base = file("."))
    .settings(inConfig(CloudTest)(Defaults.testTasks): _*)
    .dependsOn(sharedLib)
    .configs(CloudTest)

// Enable scala experimental compiler flags
scalacOptions ++= Seq("-Xexperimental")


// Enable parallel testing
parallelExecution in Test := true


def cloudFilter(name: String): Boolean = name endsWith "Cloud"
def unitFilter(name: String): Boolean = cloudFilter(name) == false

testOptions in Test := Seq(Tests.Filter(unitFilter))
testOptions in CloudTest := Seq(Tests.Filter(cloudFilter))
