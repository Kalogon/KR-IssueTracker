package ranking

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming._
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.{SparseVector, Vector}
import org.apache.spark.sql.DataFrame
import ranking.myUdf._

object Streaming {
    def operateTfIdf(words: DataFrame, batchId: Long) {
        if (words.count() != 0) {
            val hashingTF = new HashingTF()
                            .setInputCol("words")
                            .setOutputCol("TF-Features")

            val idf = new IDF()
                        .setInputCol("TF-Features")
                        .setOutputCol("Final-Features")

            val getKeyword = udf((words: Seq[String], tfIdf: Vector) => {
                tfIdf match {
                    case SparseVector(numFeatures, hashIndices, tfIdfResult) => {
                        val maxHashIndex = hashIndices(tfIdfResult.zipWithIndex.maxBy(_._1)._2)
                        val keywords = words
                            .map(word => (word, hashingTF.indexOf(word)))
                            .filter(tup => tup match {
                                case (word, hashIndex) => hashIndex == maxHashIndex
                            })
                        keywords(0)._1
                    }
                }
            })
            
            words.show()
            val tfDataFrame = hashingTF.transform(words)
            val idfModel = idf.fit(tfDataFrame)
            val tfIdfDataFrame = idfModel.transform(tfDataFrame)
                                        .withColumn("keyword", getKeyword(col("words"), col("Final-Features")))
                                        .groupBy("keyword")
                                        .count()
                                        .orderBy(desc("count"))
                                        .show(10)
        }
    }

    def main(args : Array[String]): Unit = {
        val spark = SparkSession.builder.appName("Streaming").config("spark.sql.shuffle.partitions", "240").getOrCreate()
        import spark.implicits._

        val df = spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "news")
            .load()
        
        val dataDf = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(timestamp AS LONG)")
            .as[(String, String, Long)]
        
        dataDf.printSchema()

        val tokenizer = new Tokenizer().setInputCol("value").setOutputCol("words")
        val words = tokenizer.transform(dataDf)

        val dataStream = words.writeStream
                            .foreachBatch(operateTfIdf _)
                            .outputMode("append")
                            .trigger(Trigger.ProcessingTime("1 second"))
                            .start()

        dataStream.awaitTermination()

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
