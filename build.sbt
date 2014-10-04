name := "twitter4j-tutorial"
 
version := "0.1.0 "
 
scalaVersion := "2.10.0"

val akkaVersion = "2.3.5"

libraryDependencies ++= Seq(
  "org.twitter4j" % "twitter4j-stream" % "3.0.3",
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  "com.typesafe.akka" %% "akka-agent" % akkaVersion,
  "com.typesafe.akka" %% "akka-camel" % akkaVersion excludeAll(ExclusionRule("org.apache.camel", "camel-core")),
  "com.typesafe.akka" %% "akka-stream-experimental" % "0.6",
  "net.databinder.dispatch" %% "dispatch-core" % "0.11.2",
  "com.google.protobuf" % "protobuf-java" % "2.5.0"
)