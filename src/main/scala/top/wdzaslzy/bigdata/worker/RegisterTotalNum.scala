package top.wdzaslzy.bigdata.worker

import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession

object RegisterTotalNum {
  /**
    * 分析出2018年一年注册的用户总数量
    */
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName(RegisterTotalNum.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val df = new SimpleDateFormat("yyyy-MM-dd mm:HH:ss")
    val lineRdd = sparkSession.sparkContext.textFile("E:\\workspace\\qingqingdata\\app_register_user_info.csv", 5)
    val totalRegisterCount = lineRdd.map(line => {
      val words = line.split(",")
      val entryTime = words(2)
      df.parse(entryTime).getTime
    }).filter(entryTime => {
      entryTime >= df.parse("2018-01-01 00:00:00").getTime && entryTime < df.parse("2019-01-01 00:00:00").getTime
    }).count()

    println("2018年在轻轻注册的用户一共有：" + totalRegisterCount)
  }

}
