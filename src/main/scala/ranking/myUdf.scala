package ranking

import org.apache.spark.sql.functions.udf

object myUdf {
    val most_frequent = udf((words: Seq[String]) => {
        if (words == null || words.size == 0) ""
        else words.groupBy(identity).maxBy(_._2.size)._1
    })
}