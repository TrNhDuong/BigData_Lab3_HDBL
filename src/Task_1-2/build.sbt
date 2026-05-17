name := "Task1-2"
version := "1.0"
scalaVersion := "2.12.17"

// Khai báo thư viện Hadoop
libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common"                     % "3.3.6" % "provided",
  "org.apache.hadoop" % "hadoop-mapreduce-client-core"      % "3.3.6" % "provided",
  "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % "3.3.6" % "provided",
  "org.apache.hadoop" % "hadoop-hdfs"                       % "3.3.6" % "provided"
)

// Main class
Compile / mainClass := Some("Task1_2Driver")
assembly / mainClass := Some("Task1_2Driver")

// Assembly configuration
assembly / assemblyJarName := "task1-2-assembly-1.0.jar"

// Merge strategy for assembly
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) =>
    xs match {
      case "MANIFEST.MF" :: Nil => MergeStrategy.discard
      case _                    => MergeStrategy.discard
    }
  case x                              => MergeStrategy.first
}

// Ensure manifest is created with main class
assembly / packageOptions += Package.ManifestAttributes(
  "Main-Class" -> "Task1_2Driver"
)

// Configure SBT to find source files in the 'source' directory
Compile / scalaSource := baseDirectory.value / "source"