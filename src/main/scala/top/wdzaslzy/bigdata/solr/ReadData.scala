package top.wdzaslzy.bigdata.solr

import org.apache.spark.sql.SparkSession

/**
  * @author zhongyou_li
  */
object ReadData {

  def main(args: Array[String]): Unit = {


  }

  def readSolr(): Unit ={
    val sparkSession = SparkSession.builder().appName("test").master("local[*]").getOrCreate()

    val options = Map(
      "zkHost" -> "172.20.85.45:2181,172.20.85.46:2181,172.20.85.47:2181,172.20.85.49:2181,172.20.85.59:2181/solr",
      "collection" -> "CompanyBusinessCoreInfoV2"
    )

    val df = sparkSession.read.format("solr").options(options).load

    val beanRDD = df.rdd.map(row => {
      val id = row.getAs[String]("id")
      id
    })

    println(s"一共：${beanRDD.count()} 条数据")
  }



}
