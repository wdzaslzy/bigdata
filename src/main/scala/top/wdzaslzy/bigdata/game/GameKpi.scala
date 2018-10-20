package top.wdzaslzy.bigdata.game

import org.apache.spark.sql.SparkSession

/**
  * @author lizy
  **/
object GameKpi {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().master("AverageAge").master("local[2]").getOrCreate()
    val lineRdd = sparkSession.sparkContext.textFile("E:\\gamelog.txt", 5)

    val formatRdd = lineRdd.map(line => {
      /*
      这是一个线程不安全的程序。
      在分布式环境中执行时，每个Executor下面可以有多个task，即多个线程一起执行。
      在TimeUtil中的DateFormat会被多线程共享
      因为DateFormat是线程不安全的，所以在多线程环境下，该程序会抛出错误异常
      A线程使用dateFormat正在format到某一阶段
      B线程也在用DateFormat，这样就会导致format的值不一致
       */
      TimeUtil.formatTimeToLong(line)
    })

    formatRdd.collect()

    sparkSession.close()
  }

}
