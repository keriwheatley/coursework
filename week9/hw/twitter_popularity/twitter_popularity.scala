import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
// import twitter4j.TwitterFactory
// import twitter4j.Twitter
// import twitter4j.conf.ConfigurationBuilder
import org.apache.spark.sql.SQLContext

object Main extends App {

  val startTimeMillis = System.currentTimeMillis()

  if (args.length < 4) {
    System.err.println("Usage: TwitterPopularTags <consumer key> <consumer secret> " +
      "<access token> <access token secret> <[optional] number hashtags> <[optional] sample interval in seconds> <[optional] run duration in seconds>")
    System.exit(1)
  }

  val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
  val numHashtags:Int = 8
  val sampleInterval:Int = 20
  val runDuration:Int = 180

  val numHashtagsTEST:String = if (args(4) == "") "10" else args(4)
  val sampleIntervalTEST:String = if (args(5).isEmpty) "30" else args(5)
  val runDurationTEST:String = if (args(6).isEmpty) "1800" else args(6)

  println("This is :", numHashtagsTEST)
  println("This is :", sampleIntervalTEST)
  println("This is :", runDurationTEST)

  println(s"Number hashtags: ${numHashtags}")
  println(s"Length of sample intervals (in seconds): ${sampleInterval}")
  println(s"Duration of program run (in seconds): ${runDuration}")

  System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
  System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
  System.setProperty("twitter4j.oauth.accessToken", accessToken)
  System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

  val sparkConf = new SparkConf().setAppName("TwitterPopularTags")
  val ssc = new StreamingContext(sparkConf, Seconds(1))
  val stream = TwitterUtils.createStream(ssc, None)

  val data = stream.flatMap(status => 
    status.getHashtagEntities.map(hashtag => 
      ("#"+hashtag.getText, 
        (1,
          "@"+status.getUser.getName,
          "@"+status.getUserMentionEntities().map(_.getText()).mkString("@")))))

  val aggregateFunc: ((Int, String, String), (Int, String, String)) => (Int, String, String) = {
      case ((v1, w1, y1), (v2, w2, y2)) => {
         (v1 + v2, w1 + w2, y1 + y2)
       }
    }

  val sampleCount = data.reduceByKeyAndWindow(aggregateFunc,Seconds(sampleInterval),Seconds(sampleInterval))

  sampleCount.foreachRDD(rdd => {
    val topList = rdd.sortBy(-_._2._1).take(numHashtags)
    val timeElapsed = ((1.00*(System.currentTimeMillis() - startTimeMillis)/60000 * 100).round / 100.toDouble)
    println(s"\n\nProgram time elapsed: ${timeElapsed} minutes")
    println(s"Popular hashtags in last ${sampleInterval} seconds (%s total):".format(rdd.count()))
    var rank:Int = 1
    topList.foreach{case (count, tag) => 
          {val authors = tag._2.split("@").distinct.mkString("  @")
          val mentions = tag._3.split("@").distinct.mkString("  @")
          println("\nHashtag Rank: %s\nNumber of Tweets: %s\nHashtag: %s\nAuthors:%s\nMentions:%s"
          .format(rank, tag._1, count, authors, mentions))
          rank += 1
          }}})

  val totalCount = data.reduceByKeyAndWindow(aggregateFunc,Seconds(runDuration),Seconds(runDuration))

  totalCount.foreachRDD(rdd => {
    val topList = rdd.sortBy(-_._2._1).take(numHashtags)
    val timeElapsed = ((1.00*(System.currentTimeMillis() - startTimeMillis)/60000 * 100).round / 100.toDouble)
    println(s"\n\nProgram time elapsed: ${timeElapsed} minutes")
    println(s"Most Popular hashtags for duration of program ${runDuration} seconds (%s total):".format(rdd.count()))
    var rank:Int = 1
    topList.foreach{case (count, tag) => 
          {val authors = tag._2.split("@").distinct.mkString("  @")
          val mentions = tag._3.split("@").distinct.mkString("  @")
          println("\nHashtag Rank: %s\nNumber of Tweets: %s\nHashtag: %s\nAuthors:%s\nMentions:%s"
          .format(rank, tag._1, count, authors, mentions))
          rank += 1
          }}})

  ssc.start()
  ssc.awaitTerminationOrTimeout((runDuration + 5) * 1000)
  println(s"\nMax duration of ${runDuration} seconds reached. Ending program.")
  ssc.stop()
}




