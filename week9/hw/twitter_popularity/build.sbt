lazy val common = Seq(
  organization := "week9.mids",
  version := "0.1.0",
  scalaVersion := "2.11.8",
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-streaming_2.11" % "2.2.0",
    "org.apache.spark" %% "spark-streaming-twitter_2.11" % "2.2.0",
    "com.typesafe" % "config" % "1.3.0"
  ),
  mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
     {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x => MergeStrategy.first
     }
  }
)

lazy val twitter_popularity = (project in file(".")).
  settings(common: _*).
  settings(
    name := "twitter_popularity",
    mainClass in (Compile, run) := Some("twitter_popularity.Main"))
