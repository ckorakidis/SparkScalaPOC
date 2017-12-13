
name := "SparkScalaPOC"


version := "0.0.1"

scalaVersion := "2.11.11"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.0.2"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}