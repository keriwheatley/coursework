README 
twitter_popularity.scala

This program does the following:
    1. Prints N most popular hashtags with authors and mentions in T second time windows
    2. Prints N most popular hashtags with authors and mentions over entire program duration


HOW TO RUN PROGRAM

    1. Create instance variables for your Twitter credentials
        
        export consumerKey= <consumer key>
        export consumerSecret= <consumer secret key>
        export accessToken= <access token>
        export accessTokenSecret= <access token secret>

    2. Navigate to directory:
        
        cd coursework/week9/hw/twitter_popularity/

    3. Run Spark program
        
        Syntax:
        $SPARK_HOME/bin/spark-submit --master spark://spark1:7077 $(find target -iname "*assembly*.jar") \
        $consumerKey $consumerSecret $accessToken $accessTokenSecret \
        <[optional]numberHashtags> <[optional]sampleIntervalSeconds> <[optional]runDurationSeconds>

        Example:
        $SPARK_HOME/bin/spark-submit --master spark://spark1:7077 $(find target -iname "*assembly*.jar") \
        $consumerKey $consumerSecret $accessToken $accessTokenSecret \
        5 120 900

