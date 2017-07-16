// TO RUN PROGRAM
// $SPARK_HOME/bin/spark-submit --master spark://spark1:7077 $(find target -iname "*assembly*.jar") \
//   <consumerKey> <consumerSecret> <accessToken> <accessTokenSecret> \
//   <[optional]numberHashtags> <[optional]sampleInterval> <[optional]runDuration>

// $SPARK_HOME/bin/spark-submit --master spark://spark1:7077 $(find target -iname "*assembly*.jar") \
//   $consumerKey $consumerSecret $accessToken $accessTokenSecret 5 120 170

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf

object Main extends App {

  val startTimeMillis = System.currentTimeMillis()

  if (args.length < 4) {
    System.err.println("Usage: TwitterPopularTags <consumer key> <consumer secret> " +
      "<access token> <access token secret> <[optional] number hashtags> <[optional] sample interval in seconds> <[optional] run duration in seconds>")
    System.exit(1)
  }

  val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
  var numHashtags:Int = 10
  var sampleInterval:Int = 120
  var runDuration:Int = 1800

  if (args.length > 4) {numHashtags = args(4).toInt}
  if (args.length > 5) {sampleInterval = args(5).toInt}
  if (args.length > 6) {runDuration = args(6).toInt}

  System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
  System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
  System.setProperty("twitter4j.oauth.accessToken", accessToken)
  System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

  val sparkConf = new SparkConf().setAppName("TwitterPopularTags")
  val ssc = new StreamingContext(sparkConf, Seconds(1))
  val stream = TwitterUtils.createStream(ssc, None)

  println(s"\n\n--------------------------------------")
  println(s"--------------------------------------")
  println(s"Number hashtags to display: ${numHashtags}")
  println(s"Length of sample intervals (in seconds): ${sampleInterval}")
  println(s"Duration of program run (in seconds): ${runDuration}")

  val data = stream.flatMap(status => 
    status.getHashtagEntities.map(hashtag => 
      ("#"+hashtag.getText, 
        (1,
          "@"+status.getUser.getName,
          "@"+status.getUserMentionEntities().map(_.getText()).mkString("@")))))

  val aggregateFunc: ((Int, String, String), (Int, String, String)) => (Int, String, String) = {
      case ((v1, w1, y1), (v2, w2, y2)) => {(v1 + v2, w1 + w2, y1 + y2)}}

  val sampleCount = data.reduceByKeyAndWindow(aggregateFunc,Seconds(sampleInterval),Seconds(sampleInterval))

  sampleCount.foreachRDD(rdd => {
    val topList = rdd.sortBy(-_._2._1).take(numHashtags)
    val timeElapsed = ((1.00*(System.currentTimeMillis() - startTimeMillis)/60000 * 100).round / 100.toDouble)
    println(s"\n\n--------------------------------------")
    println(s"--------------------------------------")
    println(s"Program time elapsed: ${timeElapsed} minutes")
    println(s"Popular hashtags in last ${sampleInterval} seconds (%s total)".format(rdd.count()))
    var rank:Int = 1
    topList.foreach{case (count, tag) => 
          {val authors = tag._2.split("@").distinct.mkString("  @")
          val mentions = tag._3.split("@").distinct.mkString("  @")
          if (((System.currentTimeMillis() - startTimeMillis)/1000) < runDuration) {
            println("\nHashtag Ranking: %s\nNumber of Tweets: %s\nHashtag: %s\nAuthors:%s\nMentions:%s"
          .format(rank, tag._1, count, authors, mentions))}
          rank += 1
          }}})

  val totalCount = data.reduceByKeyAndWindow(aggregateFunc,Seconds(runDuration),Seconds(runDuration))

  totalCount.foreachRDD(rdd => {
    val topList = rdd.sortBy(-_._2._1).take(numHashtags)
    val timeElapsed = ((1.00*(System.currentTimeMillis() - startTimeMillis)/60000 * 100).round / 100.toDouble)
    println(s"\n\n--------------------------------------")
    println(s"--------------------------------------")
    println(s"--------------------------------------")
    println(s"--------------------------------------")
    println(s"Program time elapsed: ${timeElapsed} minutes")
    println(s"Top Most Popular hashtags for entire program run of ${runDuration} seconds (%s total):".format(rdd.count()))
    var rank:Int = 1
    topList.foreach{case (count, tag) => 
          {val authors = tag._2.split("@").distinct.mkString("  @")
          val mentions = tag._3.split("@").distinct.mkString("  @")
          println("\nHashtag Rank: %s\nNumber of Tweets: %s\nHashtag: %s\nAuthors:%s\nMentions:%s"
          .format(rank, tag._1, count, authors, mentions))
          rank += 1
          }}})

  ssc.start()
  ssc.awaitTerminationOrTimeout((runDuration + 60) * 1000)
  println(s"\nMax duration of ${runDuration} seconds reached. Ending program.")
  ssc.stop()
}




