package top.wdzaslzy.bigdata.sparksql

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * @author lizy
  **/
object DataFrameForJdbc {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("DataFrameForJson")
      .getOrCreate()

    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "root")
    val frame = sparkSession.read.jdbc("jdbc:mysql://localhost:3306/big_data?characterEncoding=UTF-8&allowMultiQueries=true",
      "ip_access", properties)

    import sparkSession.implicits._
    val result = frame.filter($"access_count" > 1)
    result.show()

    //新表
    result.write.jdbc("jdbc:mysql://localhost:3306/big_data?characterEncoding=UTF-8&allowMultiQueries=true",
      "new_ip_access", properties)

//    frame.show()

/*    sparkSession.read.format("jdbc").options(
      Map("url" -> "",
        "" -> "")
    )*/

    sparkSession.stop()
  }

}
