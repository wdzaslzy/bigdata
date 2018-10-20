package top.wdzaslzy.bigdata.sparksql

import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import top.wdzaslzy.bigdata.tools.IpUtils

import scala.io.Source

/**
  * @author lizy
  **/
object IPComputeSQL {

  /*
  使用Spark SQL将ip的归属地进行计算
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

    //ip_rule进视图
    val sparkSession = SparkSession.builder().appName("ipCompute").master("local[4]").getOrCreate()
    import sparkSession.implicits._
    val ipRuleDataSet = sparkSession.createDataset(ipRules)
    val ipRuleDataFrame = ipRuleDataSet.toDF("beginIpNum", "endIpNum", "address")
    ipRuleDataFrame.createTempView("ip_rule")

    //ip进视图
    val ipLogRdd = sparkSession.sparkContext.textFile("E:\\IPLog.txt")
    val ipLocalRdd = ipLogRdd.map(word => {
      val ipNum = IpUtils.ipToLong(word)
      Row(ipNum)
    })
    val structType = new StructType(Array(StructField("ipNum", LongType)))

    val ipDataFrame = sparkSession.createDataFrame(ipLocalRdd, structType)
    ipDataFrame.createTempView("t_ip")

    val resultFrame = sparkSession.sql("select * from t_ip, ip_rule where ipNum <= endIpNum and ipNum >= beginIpNum")
    resultFrame.show()

    sparkSession.close()
  }

}
