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
  val numHashtags:Int = 10
  val sampleInterval:Int = 60
  val runDuration:Int = 180
  println(args(4))
  println(args(5))
  println(args(6))
  // val numHashtags:Int = if (args(4).isEmpty) 10 else args(4)
  // val sampleInterval:Int = if (args(5).isEmpty) 30 else args(5)
  // val runDuration:Int = if (args(6).isEmpty) 1800 else args(6)
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


  val statuses = tweets.map(status => status.getText())
  statuses.print()

  // val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))


  // hashTags.foreachRDD(rdd => {
  //   val topList = rdd.take(numHashtags)
  //   println(topList)
    // val timeElasped = (System.currentTimeMillis() - startTimeMillis)/1000 
    // println(s"\nList of ${numHashtags} most popular topics at ${timeElasped} seconds (%s total):".format(rdd.count()))
    // topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
  })



  // def updateFunc(values: Seq[Int], state: Option[Int]): Option[Int] = {
  //     val currentCount = values.sum
  //     val previousCount = state.getOrElse(0)
  //     Some(currentCount + previousCount)  
  // }

  // val topCounts = hashTags.map((a:Int,b:Int)).reduceByKeyAndWindow(a + b, Seconds(sampleInterval), Seconds(10))
  //                  // .map{case (topic, count) => (count, topic)}
  //                  // .transform(_.sortByKey(false))

  // topCounts.foreachRDD(rdd => {
  //   val topList = rdd.take(numHashtags)
  //   println(topList)
  //   // val timeElasped = (System.currentTimeMillis() - startTimeMillis)/1000 
  //   // println(s"\nList of ${numHashtags} most popular topics at ${timeElasped} seconds (%s total):".format(rdd.count()))
  //   // topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
  // })

  ssc.start()
  ssc.awaitTerminationOrTimeout(runDuration * 1000)
  println(s"\nMax duration reached. Ending program.")
  ssc.stop()
}


// The output of your program should be lists of hashtags that were determined 
// to be popular during the program's execution, as well as lists of users, 
// per-hashtag, who were related to them.



// The TOP rankings over the entire 30 minutes

// The top ranking over the last few minutes






