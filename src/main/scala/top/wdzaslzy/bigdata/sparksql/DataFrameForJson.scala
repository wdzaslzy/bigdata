package top.wdzaslzy.bigdata.sparksql

import org.apache.spark.sql.SparkSession

/**
  * @author lizy
  **/
object DataFrameForJson {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("DataFrameForJson")
      .getOrCreate()

    val personFrame = sparkSession.read.json("E:\\workspace\\personal\\bigdata\\src\\main\\resources\\person.json")
//    personFrame.select("name").show()

    personFrame.createTempView("person")
    sparkSession.sql("select name from person").show()

    sparkSession.close()
  }

}
