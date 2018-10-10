package top.wdzaslzy.bigdata.spark

import org.apache.spark.sql.SparkSession

/**
  * @author lizy
  **/
object CustomSort1 {

  /*
  自定义排序
   */
  def main(args: Array[String]): Unit = {
    val studentScores = Array("xiaoli 17 79", "xiaowang 16 80", "xiaozhao 18 79", "xiaozhang 16 90")
    val sparkSession = SparkSession.builder().master("local[4]").appName("CustomSort1").getOrCreate()
    val studentScoreRdd = sparkSession.sparkContext.parallelize(studentScores)
    val studentInfoRdd = studentScoreRdd.map(studentScore => {
      val studentInfos = studentScore.split(" ")
      val studentName = studentInfos(0)
      val studentAge = studentInfos(1).toInt
      val score = studentInfos(2).toInt
      new StudentInfo(studentName, studentAge, score)
    })
    val sortedRdd = studentInfoRdd.sortBy(studentInfo => studentInfo)
    val result = sortedRdd.collect()
    println(result.toBuffer)

    sparkSession.close()
  }

}

class StudentInfo(val name: String, val age: Int, val score: Int) extends Ordered[StudentInfo] with Serializable {

  override def toString: String = s"name:$name, age:$age, score:$score"

  override def compare(that: StudentInfo): Int = {
    if (this.score == that.score) {
      this.age - that.age
    } else {
      that.score - this.score
    }
  }
}
