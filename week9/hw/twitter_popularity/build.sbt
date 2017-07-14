lazy val common = Seq(
  organization := "week9.mids",
  version := "0.1.0",
  scalaVersion := "2.10.6",
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-streaming" % "2.1.1" % "provided",
    "org.apache.spark" %% "spark-streaming-twitter_2.11" % "1.5.2",
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
