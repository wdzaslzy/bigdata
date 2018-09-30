package top.wdzaslzy.bigdata.spark

import org.apache.spark.sql.SparkSession

/**
  * @author lizy
  **/
object FavoriteTeacher2 {

  /*
  每个科目下最受欢迎的老师
   */
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("FavoriteTeacher1")
      .master("local[4]")
      .getOrCreate()

    //依次读入数据，每次都1行，分3个分区
    val lineRdd = sparkSession.sparkContext.textFile("E:\\teacher.log", 3)
    val relationRdd = lineRdd.map(line => {
      val words = line.split("/")
      val teacher = words(words.length - 1)
      val subject = words(2).split("\\.")(0)
      (subject, teacher)
    })
    val relationCountRdd = relationRdd.map(relation => (relation, 1))
    val reducedRdd = relationCountRdd.reduceByKey(_ + _)
    val groupedRdd = reducedRdd.groupBy(_._1._1)
    val mapValuesRdd = groupedRdd.mapValues(_.toList.sortBy(_._2).reverse)
    val result = mapValuesRdd.collect()

    println("输出结果为：" + result.toBuffer)
    sparkSession.close()
  }

}
