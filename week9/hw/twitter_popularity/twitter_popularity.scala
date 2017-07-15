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
  val sampleInterval:Int = 2
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
  val ssc = new StreamingContext(sparkConf, Seconds(sampleInterval)) //Creates RDDs for size of sample interval
  val stream = TwitterUtils.createStream(ssc, None)

  // val totHashtagCount = scala.collection.mutable.Map[String, Int]().withDefaultValue(0)
  
  // val hashtags = stream.map {hashtag => hashtag.getHashtagEntities.map(_.getText).toList}
  //                 .flatMap(list => list)
  // hashtags.print()

  // val hashtagCount = hashtags.map(hashtag => (hashtag,1)).reduceByKey(_+_)
  // hashtagCount.print()

  val data = stream.map {line => 
        List(line.getHashtagEntities.map(_.getText).toList,
        line.getUser().getScreenName(),
        line.getUserMentionEntities.map(_.getScreenName).toList)
  }
  data.print()

  data.foreachRDD {rdd => println("Test")}  

  // data.for 

  // val result = for {
  //   hashtag <- data._1
  //   user = data._2
  //   mentionedUser <- data._3
  // } yield (hashtag, user, mentionedUser)

  // val data = stream.map { case (hashtag, user) => 
  //         (hashtag.getHashtagEntities.map(_.getText).toList,
  //           user.getUser().getScreenName()) }

  // hashtags.print()



  // val hashtagUpdate = hashtags.map {line => totHashtagCount(line) += 1}

  // hashtags.foreachRDD(rdd => rdd.map {line => println("\nTest")})

  // stream.foreachRDD(rdd => rdd.map {line => println("\nTest Stream")})

  // totHashtagCount.keys.foreach{key => 
    // print("Key = " + key)
    // println(" Value = " + totHashtagCount(key))}

  // val users = stream.map {user => user.getUser().getScreenName()}
  // users.print()
  // val mentions = stream.map {mention => mention.getUserMentionEntities.map(_.getScreenName).toList}
  //                 .flatMap(list => list)
  // mentions.print()

  // val topCounts60 = hashtags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(runDuration))
  //                    .map{case (topic, count) => (count, topic)}
  //                    .transform(_.sortByKey(false))

  // // Print popular hashtags
  // topCounts60.foreachRDD(rdd => {
  //   val topList = rdd.take(10)
  //   println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
  //   topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
  // })


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




