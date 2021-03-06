package top.wdzaslzy.bigdata.sparksql

import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import top.wdzaslzy.bigdata.tools.IpUtils

import scala.io.Source

/**
  * @author lizy
  **/
object IPComputeSQL2 {

  /*
  Spark SQL Join时，代价很大。每个Executor里面都要取拉取记录
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
    val ipRulesBroadCast = sparkSession.sparkContext.broadcast(ipRules)

    //ip进视图
    val ipLogRdd = sparkSession.sparkContext.textFile("E:\\IPLog.txt")
    val ipLocalRdd = ipLogRdd.map(word => {
      val ipNum = IpUtils.ipToLong(word)
      Row(ipNum)
    })
    val structType = new StructType(Array(StructField("ipNum", LongType)))

    val ipDataFrame = sparkSession.createDataFrame(ipLocalRdd, structType)
    ipDataFrame.createTempView("t_ip")

    //定义自定义函数，供SQL使用
    sparkSession.udf.register("ipSwitch", (ipNum: Long) => {
      IpUtils.searchLocalByIp(ipNum, ipRulesBroadCast.value)
    })

    val frame = sparkSession.sql("select ipNum, ipSwitch(ipNum) local from t_ip")
    frame.show()

    sparkSession.close()
  }

}
