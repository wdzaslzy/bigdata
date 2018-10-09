package top.wdzaslzy.bigdata.spark

import org.apache.spark.sql.SparkSession
import top.wdzaslzy.bigdata.tools.IpUtils

import scala.io.Source

/**
  * @author lizy
  **/
object IPCompute {

  /*
  ip地址计算
   */
  def main(args: Array[String]): Unit = {
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

    val sparkSession = SparkSession.builder().appName("ipCompute").master("local[4]").getOrCreate()
    //广播变量(共享在各个Executor中)
    val broadcastRef = sparkSession.sparkContext.broadcast(ipRules)
    val ipLogRdd = sparkSession.sparkContext.textFile("E:\\IPLog.txt")
    val ipLocalRdd = ipLogRdd.map(word => {
      val ipNum = IpUtils.ipToLong(word)
      val local = IpUtils.searchLocalByIp(ipNum, broadcastRef.value)
      (word, local)
    })
    val result = ipLocalRdd.collect()
    println("输出结果：" + result.toBuffer)

    sparkSession.close()
  }

}
