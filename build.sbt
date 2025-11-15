// Project Settings
name := "DeltaLakeLearningModule"
version := "0.1"

// IMPORTANT: Match the version from spark-shell output (2.13.16)
scalaVersion := "2.13.16" 

// Dependencies
libraryDependencies ++= Seq(
  // 1. Spark Dependencies (Spark 4.0.0)
  // We remove % "provided" to allow 'sbt run' to work
  "org.apache.spark" %% "spark-sql" % "4.0.0", 
  "org.apache.spark" %% "spark-core" % "4.0.0",
  "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  
  // 2. Delta Lake Dependency (Must match Spark 4.0.0)
  // This uses the official, simplified dependency line
  "io.delta" %% "delta-spark" % "4.0.0" 
)

