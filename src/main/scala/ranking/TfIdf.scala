package ranking

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming._
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.{SparseVector, Vector}
import org.apache.spark.sql.DataFrame
import ranking.myUdf._

object TfIdf {
    def operateTfIdf(words: DataFrame, batchId: Long) {
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

        val tfDataFrame = hashingTF.transform(words)
        val idfModel = idf.fit(tfDataFrame)
        val tfIdfDataFrame = idfModel.transform(tfDataFrame)
                                    .withColumn("keyword", getKeyword(col("words"), col("Final-Features")))
                                    .groupBy("keyword")
                                    .count()
                                    .orderBy(desc("count"))
                                    .show()
    }

    def main(args : Array[String]): Unit = {
        val spark = SparkSession.builder.appName("TfIdf").config("spark.sql.shuffle.partitions", "240").getOrCreate()
        // val path = "hdfs://sun01:9000/user/spark/sungwoo/csv_result/News_jb_1/News_2019/part-00000-ccfc57c4-6068-4772-b872-5ef89e768acb-c000.csv"
        val path = "hdfs://sun01:9000/user/spark/sungwoo/data_lake_test/" + "News" + "_origin_data"

        val df = spark.readStream.format("delta")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(path)

        df.createOrReplaceTempView("table")
        val dataDf = spark.sql("SELECT * FROM table WHERE news_date LIKE "+ "'2020" + "%'")
        dataDf.printSchema()

        val tokenizer = new Tokenizer().setInputCol("text_sentence").setOutputCol("words")
        val words = tokenizer.transform(dataDf)

        val streamingQuery_wordcount = words.writeStream
                                .foreachBatch(operateTfIdf _)
                                .trigger(Trigger.Once())
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
