package top.wdzaslzy.bigdata

import org.apache.spark.sql.SparkSession

/**
  * @author zhongyou_li
  */
object TestSparkSS {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("Test").master("local[*]").getOrCreate()

    val tuples = Seq(("hello", "hello1"), ("hello", "hello2"))
    val tuples2 = Seq(("hello", "Java"), ("hello", "Scala"))

    val rdd1 = sparkSession.sparkContext.parallelize(tuples)
    val rdd2 = sparkSession.sparkContext.parallelize(tuples2)

    val result = rdd1.leftOuterJoin(rdd2).collect()

    println(result.toBuffer)
  }
}
