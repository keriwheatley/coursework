import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import twitter4j.TwitterFactory
import twitter4j.Twitter
import twitter4j.conf.ConfigurationBuilder

object Main extends App {

  val startTimeMillis = System.currentTimeMillis()

  if (args.length < 4) {
    System.err.println("Usage: TwitterPopularTags <consumer key> <consumer secret> " +
      "<access token> <access token secret> <[optional] number hashtags> <[optional] sample interval in seconds> <[optional] run duration in seconds>")
    System.exit(1)
  }

  val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
  val numHashtags:Int = 10
  val sampleInterval:Int = 60
  val runDuration:Int = 180

  val numHashtagsTEST:String = if (args(4) == "") "10" else args(4)
  val sampleIntervalTEST:String = if (args(5).isEmpty) "30" else args(5)
  val runDurationTEST:String = if (args(6).isEmpty) "1800" else args(6)

  println(numHashtagsTEST)
  println(sampleIntervalTEST)
  println(runDurationTEST)

  println(s"Number hashtags: ${numHashtags}")
  println(s"Length of sample intervals (in seconds): ${sampleInterval}")
  println(s"Duration of program run (in seconds): ${runDuration}")

  System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
  System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
  System.setProperty("twitter4j.oauth.accessToken", accessToken)
  System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

  val sparkConf = new SparkConf().setAppName("TwitterPopularTags")
  val ssc = new StreamingContext(sparkConf, Seconds(10))
  val stream = TwitterUtils.createStream(ssc, None)

  val hashtags = stream.map {hashtag => hashtag.getHashtagEntities.map(_.getText).toList}
  hashtags.print()

  val users = stream.map {user => user.getUser().getScreenName()}
  users.print()

  val mentions = stream.map {mention => mention.getUserMentionEntities.map(_.getScreenName).toList}
  mentions.print()


  val topCounts60 = hashtags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(runDuration))
                     .map{case (topic, count) => (count, topic)}
                     .transform(_.sortByKey(false))

  // Print popular hashtags
  topCounts60.foreachRDD(rdd => {
    val topList = rdd.take(10)
    println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
    topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
  })

  // statuses foreach (z => println (z._1 + " : " + z._2 + " : " + z._3))
  // for ((a,b,c) <-statuses) printf("key: %s, value: %s\n",a,b)

  // val statuses = stream.map { status =>
  //   val statusAuthor = status.getUser().getScreenName()
  //   val mentionedEntities = status.getUserMentionEntities.map(_.getScreenName).toList
  //   val hashtags = status.getHashtagEntities.map(_.getText).toList
  //   // println("Author: " + statusAuthor + " Mentions" + mentionedEntities)
  // }  
  // val statuses = stream.map ( status => (status.getUser().getScreenName(),
  //   status.getUserMentionEntities.map(_.getScreenName).toList,
  //   status.getHashtagEntities.map(_.getText).toList)
  // ).toDF("author", "mentions","hashtag").writeStream

  // statuses.print()
  // data.foreachRDD(rdd => {
  //   val topList = rdd.take(10)
  //   println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
  //   topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
  // })

  // data.print()



  ssc.start()
  ssc.awaitTerminationOrTimeout(runDuration * 1000)
  println(s"\nMax duration reached. Ending program.")
  ssc.stop()
}




