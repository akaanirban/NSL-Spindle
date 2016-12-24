organization := "edu.rpi.cs.nsl.spindle"
scalaVersion := "2.11.8"

version := "1.3.0"
name := "shared-lib"

resolvers += "softprops-maven" at "http://dl.bintray.com/content/softprops/maven"

// Logging
libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.21"
// Unit Testing Library
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % "test"
// Zookeeper
libraryDependencies += "org.apache.zookeeper" % "zookeeper" % "3.4.9"
// Docker
libraryDependencies += "com.spotify" % "docker-client" % "3.5.12"
