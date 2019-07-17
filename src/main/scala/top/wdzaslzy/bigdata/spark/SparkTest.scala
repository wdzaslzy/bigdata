package top.wdzaslzy.bigdata.spark

import org.apache.spark.sql.SparkSession

/**
  * @author zhongyou_li
  */
object SparkTest {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().master("local[*]").appName("Test").getOrCreate()
    val lineRdd = sparkSession.sparkContext.textFile("E:\\行业分类.xlsx")
    val lines: Array[String] = lineRdd.collect()
    lines.foreach(line => {
      println(line)
    })

  }

}
