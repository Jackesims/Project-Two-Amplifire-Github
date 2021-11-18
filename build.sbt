import Dependencies._

// Change of scalaVersion to match spark example sbt build
ThisBuild / scalaVersion     := "2.11.8"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

lazy val root = (project in file("."))
  .settings(
    name := "P2-Amplifire",
    libraryDependencies += scalaTest % Test
	  ,libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3"
    ,libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3",
    libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.0"
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
