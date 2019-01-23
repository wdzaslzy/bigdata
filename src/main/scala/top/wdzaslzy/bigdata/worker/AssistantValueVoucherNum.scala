package top.wdzaslzy.bigdata.worker

import java.sql.DriverManager
import java.util.Date

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.SparkSession

/**
  * @author lizy
  **/
object AssistantValueVoucherNum {

  /**
    * 统计助教当月可送券数量
    */
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("AssistantValueVoucherNum").master("local[*]").getOrCreate()
    val assistantInfoRdd = getAssistantInfo(sparkSession)
    val hireDayRdd = assistantInfoRdd.filter(_._2 > 0L)
      .coalesce(2)
      .map(info => {
        val hireDay = (new Date().getTime - info._2) / (60 * 60 * 24 * 1000)
        (info._1, hireDay)
      })

    val result = hireDayRdd.collect()
    println(result.toBuffer)
    sparkSession.close()
  }

  def getAssistantInfo(sparkSession: SparkSession): JdbcRDD[(Long, Long)] = {
    val jdbcRdd: JdbcRDD[(Long, Long)] = new JdbcRDD(sparkSession.sparkContext, () => DriverManager.getConnection("jdbc:mysql://mysql.tst.idc.cedu.cn:5629/qq_user?characterEncoding=UTF-8&allowMultiQueries=true",
      "qingqingtst", "qingqing@tst"), "select id, hire_time from t_assistant where id > ? and id < ?",
      0L, Long.MaxValue, 4, rowMap => {
        val id = rowMap.getLong(1)
        val date = rowMap.getDate(2)
        var hireTime = 0L
        if (date != null) {
          hireTime = date.getTime
        }
        (id, hireTime)
      })
    jdbcRdd
  }

}
