import sbt.Keys._

name := "ClusterWild"

version := "1.0"

scalaVersion := "2.10.4"

//libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.0-SNAPSHOT"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.2.1"

//libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.3.0-SNAPSHOT"

libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.2.1"

libraryDependencies += "log4j" % "log4j" % "1.2.14"
