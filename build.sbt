val typeSafeConfig = "com.typesafe"                 % "config"              % "1.3.4"
val hadoopCommon   = "org.apache.hadoop"            % "hadoop-common"       % "3.2.0"
val hadoopClient   = "org.apache.hadoop"            % "hadoop-client"       % "3.2.0"

libraryDependencies ++= Seq(typeSafeConfig, hadoopCommon, hadoopClient)

lazy val root = (project in file(".")).
  settings(
    name := "hdfs-reader",
    version := "0.1",
    scalaVersion := "2.12.0",
    mainClass in (Compile, run) := Some("com.knoldus.util.HDFSReader")
  )
