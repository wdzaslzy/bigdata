package top.wdzaslzy.bigdata.spark

import org.apache.spark.sql.SparkSession

/**
  * @author lizy
  **/
object AverageAge {

  /*
  求文件中的平均年龄。文件第一列为序号，第二列为年龄值
   */
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("AverageAge").master("local[2]").getOrCreate()
    val lineRdd = sparkSession.sparkContext.textFile("E:\\age.txt", 5)

    val ageRdd = lineRdd.map(line => line.split(" ")(1).toInt)
    val ageTotal = ageRdd.reduce(_ + _)
    val count = ageRdd.count()
    println("总年龄：" + ageTotal + " 总人数:" + count + " 平均年龄：" + ageTotal / count)
    sparkSession.close()
  }

}
