package top.wdzaslzy.bigdata.sparksql

import org.apache.spark.sql.SparkSession

object HiveTest {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("HiveTest")
      .getOrCreate()

    val rdds = sparkSession.sparkContext.parallelize(Array(1, 2, 4, 5, 6))
    val tupleRdd = rdds.map(x => {
      (x, (x * 2).toString)
    })

    import sparkSession.implicits._
    val dataFrame = tupleRdd.toDF("id", "name")
    dataFrame.createTempView("table")

    sparkSession.sql("select * from table").show()

    sparkSession.close()
  }

}
