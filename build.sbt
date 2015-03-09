import sbt.Keys._

name := "ClusterWild"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.2.0"

libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.2.0"

libraryDependencies += "log4j" % "log4j" % "1.2.14"
