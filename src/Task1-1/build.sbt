name := "task1-1-hadoop"
version := "1.0"
scalaVersion := "2.12.17"

// Hadoop 3.3.6 dependencies (provided = already on cluster classpath)
libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common"           % "3.3.6" % "provided",
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.3.6" % "provided",
  "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % "3.3.6" % "provided",
  "org.apache.hadoop" % "hadoop-hdfs"             % "3.3.6" % "provided"
)

// Build a fat JAR (no Hadoop deps bundled since they are provided)
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _                        => MergeStrategy.first
}

// Main class
Compile / mainClass := Some("Task1_1Driver")
assembly / mainClass := Some("Task1_1Driver")
