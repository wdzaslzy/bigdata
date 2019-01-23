package top.wdzaslzy.bigdata.worker

import org.apache.spark.sql.SparkSession
import top.wdzaslzy.bigdata.tools.IpUtils

import scala.io.Source

object RegisterTotalNumByRegion {

  /*
      根据地区，分析出轻轻所有用户分布
   */
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    //第一步，获取IP规则
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

    //第二步：将IP规则广播到每个Executor中
    val broadcastIpRules = sparkSession.sparkContext.broadcast(ipRules)

    //第三步：读取数据文件
    val userInfoRdd = sparkSession.sparkContext.textFile("E:\\workspace\\qingqingdata\\app_register_user_info.csv", 3)
    val resultTupls = userInfoRdd.map(userInfo => {
      val infos = userInfo.split(",")
      val ipStr = infos(4)
      val userAddress = IpUtils.searchLocalByIp(IpUtils.ipToLong(ipStr), broadcastIpRules.value)
      (userAddress, 1)
    }).reduceByKey(_ + _).collect()

    println("在轻轻注册的用户按地区分为：" + resultTupls.toBuffer)
  }


}
