package top.wdzaslzy.bigdata.sparksql

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}


/**
  * @author lizy
  **/
object SqlDemo {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("SQLDemo").master("local[*]").getOrCreate()
    val ageRdd = sparkSession.sparkContext.textFile("E:\\age.txt")
    val infoRdd = ageRdd.map(line => {
      val oneLine = line.split(" ")
      val id = oneLine(0).toInt
      val age = oneLine(1).toInt
      Row(id, age)
    })
    val schema = StructType(List(StructField("id", IntegerType, nullable = true),
      StructField("age", IntegerType, nullable = true)))

    val dataFrame = sparkSession.createDataFrame(infoRdd, schema)
    dataFrame.show()
    sparkSession.close()
  }

}
