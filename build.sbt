
ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.3.3"

lazy val root = (project in file("."))
  .settings(
    name := "CS441_HW1"
  )

ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", "services", xs @ _*) => MergeStrategy.concat
  case PathList("META-INF", _*)                  => MergeStrategy.discard
  case PathList("reference.conf")                => MergeStrategy.concat
  case PathList("application.conf")              => MergeStrategy.concat
  case _                                         => MergeStrategy.first
}

//libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.3.6"
//// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-mapreduce-client-core
//libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.3.6"
//libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % "3.3.6"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common" % "3.3.6",
  "org.apache.hadoop" % "hadoop-hdfs" % "3.3.6",
  "org.apache.hadoop" % "hadoop-client" % "3.3.6",
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.3.6",
  "org.apache.hadoop" % "hadoop-aws" % "3.3.6",  // AWS integration in Hadoop
  "com.amazonaws" % "aws-java-sdk" % "1.12.765"   // AWS SDK for interacting with AWS services
)

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % Test
libraryDependencies += "ch.qos.logback" % "logback-core" % "1.5.6"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.5.6"
libraryDependencies += "org.slf4j" % "slf4j-api" % "2.0.12"
libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "2.0.13"

libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.23.1"
libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.23.1"
libraryDependencies += "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.23.1"
libraryDependencies += "com.typesafe" % "config" % "1.4.3"
libraryDependencies += "com.knuddels" % "jtokkit" % "1.1.0"
libraryDependencies += "org.deeplearning4j" % "deeplearning4j-core" % "1.0.0-M2.1"
libraryDependencies += "org.deeplearning4j" % "deeplearning4j-nlp" % "1.0.0-M2.1"
libraryDependencies += "org.nd4j" % "nd4j-native-platform" % "1.0.0-M2.1"
libraryDependencies += "com.typesafe" % "config" % "1.4.3"