package org.inceptez.hackstreaming

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext  
import org.apache.spark.rdd._
import org.apache.spark.sql.hive._


object filestream {
  def main(args:Array[String])
  {
    // Create the context    
    val spark = SparkSession.builder().appName("sparkStreamingHackthon").master("local[*]")
      .config("spark.history.fs.logDirectory", "file:///tmp/spark-events")
      .config("spark.eventLog.dir", "file:////tmp/spark-events")
      .config("spark.eventLog.enabled", "true")
      .config("hive.metastore.uris","thrift://localhost:9083")
      .config("spark.sql.warehouse.dir","hdfs://localhost:54310/user/hive/warehouse")
      .enableHiveSupport().getOrCreate();
    val sc = spark .sparkContext;
    
    sc.setLogLevel("ERROR") 
    
    // 1. Load the chatdata into a Dataframe using ~ delimiter by pasting in kafka or streaming file source or socket with the seconds of 50.
    val ssc = new StreamingContext(sc, Seconds(50))
    val chatdstream = ssc.textFileStream("file:///home/hduser/sparkdata/streaming/")        
    
    import spark.implicits._;
    
    chatdstream.foreachRDD { rdd =>
      if (!rdd.isEmpty())
      {
        var chatdf = rdd.map(_.split("~")).map ( x => (x(0).toInt,x(1).trim(),x(2).trim())).toDF();
        
        // 2. Define the column names as id,chat,'type'
        chatdf= chatdf.withColumnRenamed("_1", "id").withColumnRenamed("_2", "chat").withColumnRenamed("_3", "type");

        // 3. Filter only the records contains 'type' as 'c
        var filterChat = chatdf.filter("type='c'");
        filterChat.show(1);
        
        // 4. Remove the column 'type' from the above dataframe, hence the resultant dataframe contains only id and chat and convert to tempview.
        filterChat.drop("type").createOrReplaceTempView("tempview");
        
        /* 5. Use SQL split function on the 'chat' column with the delimiter as ' ' (space) to tokenize all words for
          eg. if a chat looks like this (i have internet issue from yesterday) and convert the chat column as array
          type and name it as chat_tokens which has to looks like [i,have,internet,issue,from,yesterday]
          id,chat_tokens
          1,[i,have,internet,issue,from,yesterday] */
        
        val temp1 = spark.sql("select id, split(chat,' ') as chat_tokens from tempview");
        temp1.createOrReplaceTempView("chattokenview");
        println("chattokenview: " + temp1.show(3));
        
        /*  6. Use SQL explode function to pivot the above data created in step 5, then the exploded/pivoted data
        looks like below, register this as a tempview.
        id,chat_splits
        1,i
        1,have  */
        val temp2 = spark.sql("select id, explode(chat_tokens) as chat_splits from chattokenview")
        temp2.createOrReplaceTempView("chattempview")        
        println("chattempview: " + temp2.show(3));
        // 7 missing in dec
        
        /* 8. Load the stopwords from linux file location /home/hduser/stopwordsdir/stopwords (A single column
          file found in the web that contains values such as (a,above,accross,after,an,as,at) etc. which is used in
          any English sentences for punctuations or as fillers) into dataframe with the column name stopword and
          convert to tempview.  */
        
        val temp3 = spark.read.text("file:///home/hduser/install/IZ_WORKOUTS/SPARK_HACKATHON_2020/realtimedataset/stopwords").toDF("stopword");
        temp3.createOrReplaceTempView("stopwordtempview");
        println("stopwordtempview: " + temp3.show(3));
        
        /* 9. Write a left outer join between chat tempview created in step 6 and stopwords tempview created in
          step 8 and filter all nulls (or) use subquery with not in option to filter all stop words from the actual chat
          tempview using the stopwords tempview created above. */
        val joneddata = spark.sql("select c.id,c.chat_splits from chattempview c left outer join stopwordtempview s on c.chat_splits=s.stopword where s.stopword is null");
        println("joneddata: " + joneddata.show(3));
        
        // 10. Load the final result into a hive table should have only result as given below using append option.                       
        spark.sql("create database if not exists hackathon");        
        joneddata.write.mode("append").saveAsTable("hackathon.chatstopword");
        
        /* 11. Identify the most recurring keywords used by the customer in all the chats by grouping based on the
          keywords used with count of keywords. use group by and count functions in the sql
          for eg.
          issues,2
          notworking,2
          tv,2     */
        
        val chatKeywords = spark.sql("select chat_splits, count(1) as count from hackathon.chatstopword group by chat_splits");
        println("chatKeywords: " + chatKeywords.show(3));
        // 12. Store the above result in a json format with column names as chatkeywords, occurance

        chatKeywords.toDF("chatkeywords","occurance").coalesce(1).write.mode("overwrite").json("hdfs://localhost:54310/user/hduser/hackathon/chatKeywordsoccurancejson");            
       }       
    } // end of for each
    
    ssc.start()
    ssc.awaitTermination()
  }
}