package top.wdzaslzy.bigdata.spark

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.SparkSession

/**
  * @author lizy
  **/
object JdbcRdd {

  /*
  从JDBC里面去获取RDD
   */
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName("JDBCRDD").getOrCreate()

    //创建JDBCRDD
    val jdbcRdd = new JdbcRDD(sparkSession.sparkContext, () => {
      DriverManager.getConnection("jdbc:mysql://localhost:3306/big_data?characterEncoding=UTF-8&allowMultiQueries=true", "root", "root")
    }, "select * from ip_access where id >= ? and id < ?", 1L, 10L, 2, rowMap => {
      val id = rowMap.getInt(1)
      val address = rowMap.getString(2)
      val count = rowMap.getInt(3)
      (address, count)
    })
    val result = jdbcRdd.collect()
    println(result.toBuffer)

    sparkSession.close()
  }

}
