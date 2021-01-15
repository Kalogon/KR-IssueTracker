package ranking

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.functions._
import ranking.myUdf._

object Producer {
    def main(args : Array[String]): Unit = {

        val spark = SparkSession
                    .builder
                    .appName("Producer")
                    .getOrCreate()

        val path = "hdfs://sun10:9000/user/spark/geonho/news"

        val schema = new StructType()
                        .add("news_name", StringType)
                        .add("news_time", IntegerType)
                        .add("topic", StringType)
                        .add("publisher", StringType)
                        .add("news_title", StringType)
                        .add("news_sentence", StringType)
                        .add("url", StringType)

        val df = spark.read
            .format("csv")
            .schema(schema)
            .load(path)

        val checkpointDir = "hdfs://sun10:9000/user/spark/geonho/checkpoint"
    
        df.printSchema()

        val ds = df.selectExpr("CAST(news_name AS STRING) as key", "CAST(news_sentence AS STRING) as value")
                .write
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("topic", "news")
                // .option("checkpointLocation", checkpointDir)
                .save()
    }
}