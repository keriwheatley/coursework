package org.apache.spark.examples.streaming

import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingExamples}
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf

// Number hastags display, Sample interval, App Duration
object Main extends App {
  println(s"I got executed with ${args size} args, they are: ${args mkString ", "}")

  if (args.length < 4) {
    System.err.println("Usage: TwitterPopularTags <consumer key> <consumer secret> " +
      "<access token> <access token secret> [<filters>]")
    System.exit(1)
  }

  StreamingExamples.setStreamingLogLevels()

  val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
  val filters = args.takeRight(args.length - 4)

  // Set the system properties so that Twitter4j library used by twitter stream
  // can use them to generat OAuth credentials
  System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
  System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
  System.setProperty("twitter4j.oauth.accessToken", accessToken)
  System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

  val sparkConf = new SparkConf().setAppName("TwitterPopularTags")
  val ssc = new StreamingContext(sparkConf, Seconds(2))
  val stream = TwitterUtils.createStream(ssc, None, filters)

  val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

  val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
                   .map{case (topic, count) => (count, topic)}
                   .transform(_.sortByKey(false))

  val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
                   .map{case (topic, count) => (count, topic)}
                   .transform(_.sortByKey(false))


  // Print popular hashtags
  topCounts60.foreachRDD(rdd => {
    val topList = rdd.take(10)
    println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
    topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
  })

  topCounts10.foreachRDD(rdd => {
    val topList = rdd.take(10)
    println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
    topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
  })

  ssc.start()
  ssc.awaitTermination()
}


// The output of your program should be lists of hashtags that were determined 
// to be popular during the program's execution, as well as lists of users, 
// per-hashtag, who were related to them.
