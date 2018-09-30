package top.wdzaslzy.bigdata.spark

import org.apache.spark.sql.SparkSession

/**
  * @author lizy
  **/
object WordCount {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[4]").appName("wordCount").getOrCreate()
    val lineRdd = sparkSession.sparkContext.textFile("E:\\hello.txt", 2)
    val wordRdd = lineRdd.flatMap(line => line.split(","))
    val wordMapRdd = wordRdd.map(word => (word, 1))
    val reduceRdd = wordMapRdd.reduceByKey(_ + _)
    val result = reduceRdd.collect()
    println("输出结果为：" + result.toBuffer)
    sparkSession.close()
  }

}
