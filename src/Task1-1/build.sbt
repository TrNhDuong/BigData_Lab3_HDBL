name := "Task1-1-SlidingWindow"

version := "1.0"

scalaVersion := "2.12.18"

val sparkVersion = "3.3.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided"
)

// Assembly settings: create a fat JAR
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _                        => MergeStrategy.first
}

// Main class
Compile / mainClass := Some("Task1_1")
