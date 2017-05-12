organization := "edu.rpi.cs.nsl.spindle"
scalaVersion := "2.11.8"

version := "1.4.0"
name := "shared-lib"

resolvers += "softprops-maven" at "http://dl.bintray.com/content/softprops/maven"

// Kafka
libraryDependencies += "org.apache.kafka" %% "kafka" % "0.10.2.0"
// Logging
libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.21"
// Unit Testing Library
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % "test"
// Zookeeper
libraryDependencies += "org.apache.zookeeper" % "zookeeper" % "3.4.9"
libraryDependencies += "com.101tec" % "zkclient" % "0.10"
// Docker
libraryDependencies += "com.spotify" % "docker-client" % "3.5.12"

// https://mvnrepository.com/artifact/org.scala-lang/scala-reflect
libraryDependencies += "org.scala-lang" % "scala-reflect" % "2.11.8"
