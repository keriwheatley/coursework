lazy val common = Seq(
  organization := "week9.mids",
  version := "0.1.0",
  scalaVersion := "2.10.5",
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-streaming" % "1.6.3" % "provided",
    "org.apache.spark" %% "spark-streaming-twitter" % "1.6.3",
    "com.typesafe" % "config" % "1.3.0",
    "org.apache.spark" %% "spark-sql" % "1.6.3"
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
