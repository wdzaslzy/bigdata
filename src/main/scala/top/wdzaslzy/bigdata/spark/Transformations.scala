package top.wdzaslzy.bigdata.spark

import org.apache.spark.sql.SparkSession

/**
  * @author lizy
  **/
object Transformations {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("study transformations")
      .master("local[4]")
      .getOrCreate()

    val rdd1 = sparkSession.sparkContext.parallelize(List(1, 2, 3, 4, 5, 6, 6), 3)
    //map 函数，通过一个函数，返回一个新的RDD
    val rdd2 = rdd1.map(_ * 2)      //2,4,6,8,10,12,12
    //distinct 函数，去重
    val rdd3 = rdd2.distinct()      //2,4,6,8,10,12
    //union 合并两个RDD
    val newRdd = sparkSession.sparkContext.parallelize(List(7, 8, 9))
    val rdd4 = rdd3.union(newRdd)   //2,4,6,8,10,12,7,8,9
    //filter  过滤
    val rdd5 = rdd4.filter(_ % 2 == 0)    //2,4,6,8,10,12,8
    val result: Array[Int] = rdd5.collect()
    println("输出结果为：" + result.toBuffer)

    sparkSession.close()
  }

}
