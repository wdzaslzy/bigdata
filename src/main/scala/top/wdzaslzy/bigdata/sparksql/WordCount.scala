package top.wdzaslzy.bigdata.sparksql

import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * @author lizy
  **/
object WordCount {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("WordCount")
      .getOrCreate()

    val lineSet: Dataset[String] = sparkSession.read.textFile("E:\\hello.txt")
    import sparkSession.implicits._
    val wordSet: Dataset[String] = lineSet.flatMap(line => {
      line.split(" ")
    })

    val frame = wordSet.toDF("word")
    frame.createTempView("t_words")

    sparkSession.sql("select word, count(word) as count from t_words group by word").show()

    sparkSession.close()
  }
}
