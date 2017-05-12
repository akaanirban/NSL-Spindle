organization := "net.kronmiller.william"
version := "0.0.1"
scalaVersion := "2.11.8"

name := "TestSpark"

//libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.1"
//libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.0.1"
//libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.0.1"

libraryDependencies += "org.apache.kafka" %% "kafka" % "0.10.2.0"

// Logging
libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.21"

lazy val sharedLib = RootProject(file("../Shared"))
lazy val sparkLib = RootProject(file("../Spark"))
lazy val vehicleCode = RootProject(file("../Vehicle/Vehicle-Node"))

val main = Project(id = "TestSpark", base = file("."))
  .dependsOn(sharedLib)
  .dependsOn(sparkLib)
  .dependsOn(vehicleCode)
// Enable scala experimental compiler flags
scalacOptions ++= Seq("-Xexperimental", "-feature", "-deprecation", "-language:postfixOps")

// Enable parallel testing
parallelExecution in Test := false

fork := true
