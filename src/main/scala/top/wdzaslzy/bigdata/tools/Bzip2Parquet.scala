package top.wdzaslzy.bigdata.tools

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * @author lizy
  **/
object Bzip2Parquet {

  /**
    * 将原始日志文件转换成parquet格式
    * 采用snappy压缩方式
    */
  def main(args: Array[String]): Unit = {

    /*
    1. 接受程序参数
    2. 校验参数是否合法
    3. 创建SparkSession
    4. 读取数据
    5. 转换为parquet格式
     */
    if (args.length != 2) {
      println(
        """
          |top.wdzaslzy.bigdata.tools.Bzip2Parquet
          |参数：
          |   logInputPath
          |   resultOutputPath
          |""".stripMargin)
      sys.exit()
    }

    val logInputPath = args(0)
    val resultOutputPath = args(1)

    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Bzip2Parquet")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val rowData = sparkSession.sparkContext.textFile(logInputPath)

    val rdd: RDD[String] = rowData.filter(_.split(",", -1).length >= 85)
      .flatMap(_.split(",", -1))
    val logStructType = StructType(Seq(StructField("", StringType)))



  }

}
