organization := "edu.rpi.cs.nsl"
scalaVersion := "2.11.8"

version := "0.0.2"
name := "Spindle"

resolvers += "softprops-maven" at "http://dl.bintray.com/content/softprops/maven"

// Logging
libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.21"
// Spark
//libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.1" % "provided"
//libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.0.1" % "provided"
//libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.0.1" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.1"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.0.1"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.0.1"

// Kafka uitls
libraryDependencies += "org.apache.kafka" %% "kafka" % "0.10.2.0"
// Unit Testing Library
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % "test"
// Zookeeper
libraryDependencies += "org.apache.zookeeper" % "zookeeper" % "3.4.9"
//libraryDependencies += "edu.rpi.cs.nsl.spindle" %% "shared-lib" % "1.4.0"
lazy val sharedLib = RootProject(file("../Shared"))
val main = Project(id = "NSL-Spark", base = file(".")).dependsOn(sharedLib)


// Spark Test Configurations
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
parallelExecution in Test := false


//Scaladoc and gh-pages configurations
site.includeScaladoc()

ghpages.settings
git.remoteRepo := "git@github.com:wkronmiller/NSL-SubscriptionSubsumption.git"

excludeFilter in GhPagesKeys.cleanSite :=
  new FileFilter {
    def accept(f: File) = f.getCanonicalPath == "index.html"
  }
