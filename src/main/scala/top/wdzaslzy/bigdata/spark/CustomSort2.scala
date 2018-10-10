package top.wdzaslzy.bigdata.spark

import org.apache.spark.sql.SparkSession

/**
  * @author lizy
  **/
object CustomSort2 {

  def main(args: Array[String]): Unit = {
    val studentScores = Array("xiaoli 17 79", "xiaowang 16 80", "xiaozhao 18 79", "xiaozhang 16 90")
    val sparkSession = SparkSession.builder().master("local[4]").appName("CustomSort1").getOrCreate()
    val studentScoreRdd = sparkSession.sparkContext.parallelize(studentScores)
    val studentInfoRdd = studentScoreRdd.map(studentScore => {
      val studentInfos = studentScore.split(" ")
      val studentName = studentInfos(0)
      val studentAge = studentInfos(1).toInt
      val score = studentInfos(2).toInt
      (studentName, studentAge, score)
    })
    //使用元组的比较规则
    val sortedRdd = studentInfoRdd.sortBy(studentInfo => (-studentInfo._3, studentInfo._2))
    val result = sortedRdd.collect()
    println(result.toBuffer)

    sparkSession.close()
  }

}
