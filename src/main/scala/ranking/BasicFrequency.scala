package ranking

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming._
import ranking.myUdf._

object BasicFrequency {
    def main(args : Array[String]): Unit = {
        val spark = SparkSession.builder.appName("BasicFrequency").config("spark.sql.shuffle.partitions", "240").getOrCreate()
        // val path = "hdfs://sun01:9000/user/spark/sungwoo/csv_result/News_jb_1/News_2019/part-00000-ccfc57c4-6068-4772-b872-5ef89e768acb-c000.csv"
        val path = "hdfs://sun01:9000/user/spark/sungwoo/data_lake_test/" + "News" + "_origin_data"

        val df = spark.readStream.format("delta")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(path)

        df.createOrReplaceTempView("table")
        val dataDf = spark.sql("SELECT * FROM table WHERE news_date LIKE "+ "'2020" + "%'")
        dataDf.printSchema()

        
        val words = dataDf.withColumn("words", split(col("text_sentence"), "\\s"))
        val count = words.withColumn("frequent_word", most_frequent(col("words")))
                        .groupBy("frequent_word")
                        .count()
                        .orderBy(desc("count"))

        val streamingQuery_wordcount = count.writeStream
                                .format("console")
                                .outputMode("complete")
                                .trigger(Trigger.ProcessingTime("1 second"))
                                .start()

        streamingQuery_wordcount.awaitTermination()

        // val wordsDF = dataDf.select(split(col("text_sentence"), "\\s").as("words"))
        // val wordEachDF = wordsDF.groupBy("words")
        // val wordDF = wordSetsDF.select(explode(col("wordSets")).as("word"))
        // val count = wordDF.groupBy("word").count().orderBy(desc("count"))


        // val wordSetsDF = dataDf.select(array_distinct(split(col("text_sentence"), "\\s")).as("wordSets"))
        // val wordDF = wordSetsDF.select(explode(col("wordSets")).as("word"))
        // val count = wordDF.groupBy("word").count().orderBy(desc("count"))

        // val wordsDF = dataDf.select(split(col("text_sentence"), "\\s").as("words"))
        // val wordDF = wordsDF.select(explode(col("words")).as("word"))
        // val count = wordDF.groupBy("word").count().orderBy(desc("count"))
       
        
        // val streamingQuery_wordcount = count.writeStream
        //                         .format("console")
        //                         .outputMode("complete")
        //                         .trigger(Trigger.ProcessingTime("1 second"))
        //                         .start()

        // streamingQuery_wordcount.awaitTermination()
    }
}
