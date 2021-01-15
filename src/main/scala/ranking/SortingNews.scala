package ranking

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.functions._
import java.util.Properties
import scala.io.Source


object SortingNews {
    def main(args: Array[String]) {
        val spark = SparkSession
                    .builder
                    .appName("SortingNews")
                    .getOrCreate()

        val readPath = "hdfs://sun01:9000/user/spark/sungwoo/data_lake_test/" + "News" + "_origin_data"
        val writePath = "hdfs://sun10:9000/user/spark/geonho/news"

        val df = spark.read.format("delta")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(readPath)

        df.createOrReplaceTempView("table")
        val dataDf = spark.sql("SELECT * FROM table WHERE news_date LIKE "+ "'2020" + "%'")
        val info = dataDf.orderBy(asc("news_date"))

        info.coalesce(1)
            .write
            .format("csv")
            .save(writePath)
    }
}