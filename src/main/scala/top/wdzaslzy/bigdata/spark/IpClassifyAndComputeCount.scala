package top.wdzaslzy.bigdata.spark

import java.sql.DriverManager

import org.apache.spark.sql.SparkSession
import top.wdzaslzy.bigdata.tools.IpUtils

import scala.io.Source

/**
  * @author lizy
  **/
object IpClassifyAndComputeCount {

  /*
  ip根据归属地分类，并计算每个归属地的访问量
   */
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("ipCompute").master("local[4]").getOrCreate()
    val source = Source.fromFile("E:\\IPData.txt", "GBK")
    val lines = source.getLines().toList
    val ipRules = new Array[(Long, Long, String)](lines.size)
    var index = 0
    for (line <- lines) {
      val words = line.split("\\\t")
      val ipBegin = words(0)
      val ipEnd = words(1)
      val address = words(2)
      val beginIpNum = IpUtils.ipToLong(ipBegin)
      val endIpNum = IpUtils.ipToLong(ipEnd)
      ipRules(index) = (beginIpNum, endIpNum, address)
      index += 1
    }

    //广播变量(共享在各个Executor中)
    val broadcastRef = sparkSession.sparkContext.broadcast(ipRules)
    val ipLogRdd = sparkSession.sparkContext.textFile("E:\\IPLog.txt")
    val ipLocalMapRdd = ipLogRdd.map(word => {
      val ipNum = IpUtils.ipToLong(word)
      val local = IpUtils.searchLocalByIp(ipNum, broadcastRef.value)
      (local, 1)
    })
    val reducedRdd = ipLocalMapRdd.reduceByKey(_ + _)

    //要将数据保存在MySQL中
    reducedRdd.foreachPartition(tuples => {
      //写入到MySQL
      val connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/big_data?characterEncoding=UTF-8&allowMultiQueries=true", "root", "root")
      val insertSQL = "insert into ip_access(address, access_count) value (?, ?)"
      val statement = connection.prepareStatement(insertSQL)
      tuples.foreach(tuple => {
        statement.setString(1, tuple._1)
        statement.setInt(2, tuple._2)
        statement.executeUpdate()
      })
      statement.close()
      connection.close()
    })

    sparkSession.close()
  }

}
