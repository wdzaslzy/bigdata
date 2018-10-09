package top.wdzaslzy.bigdata.tools

/**
  * @author lizy
  **/
object IpUtils {
  def ipToLong(ip: String): Long = {
    val words = ip.split("\\.")
    var result = 0L
    var index = 0
    words.foreach(word => {
      val long = word.toLong
      result |= long << ((3 - index) << 3)
      index += 1
    })
    result
  }

  //二分查找法
  def searchLocalByIp(ip: Long, ipRules: Array[(Long, Long, String)]): String = {
    var beginIndex = 0
    var endIndex = ipRules.length
    var result = ""
    var boolIndex = true
    while (beginIndex <= endIndex && boolIndex) {
      val index = (beginIndex + endIndex) / 2
      val beginIpNum = ipRules(index)._1
      val endIpNum = ipRules(index)._2
      if (ip < beginIpNum) {
        endIndex = index
      }
      if (ip > endIpNum) {
        beginIndex = index
      }

      if (ip > beginIpNum && ip < endIpNum) {
        result = ipRules(index)._3
        boolIndex = false
      }

    }
    result
  }

}
