// GHPages Publisher
resolvers += "jgit-repo" at "http://download.eclipse.org/jgit/maven"
addSbtPlugin("com.typesafe.sbt" % "sbt-ghpages" % "0.6.2")
// Code Linting
addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")
libraryDependencies ++= Seq(
  "org.mockito" % "mockito-core" % "2.8.47"
)
