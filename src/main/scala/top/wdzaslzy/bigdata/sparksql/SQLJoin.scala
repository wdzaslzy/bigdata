package top.wdzaslzy.bigdata.sparksql

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.Predef

/**
  * @author lizy
  **/
object SQLJoin {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("SQLJoin")
      .master("local[*]")
      .getOrCreate()

    import sparkSession.implicits._
    val dataSet = sparkSession.createDataset(List("1,xiaowang,china", "2,xiaozhang,us", "3,xiaojin,hk"))

    import sparkSession.implicits._
    val userInfoDataSet: Dataset[(Long, String, String)] = dataSet.map(data => {
      val line = data.split(",")
      val id = line(0).toLong
      val name = line(1)
      val nation = line(2)
      (id, name, nation)
    })
    val userDataFrame = userInfoDataSet.toDF("id", "name", "nation")

    import sparkSession.implicits._
    val nationDataSet = sparkSession.createDataset(List("china,中国", "us,美国"))
    val nationInfoDataSet = nationDataSet.map(data => {
      val info = data.split(",")
      val ename = info(0)
      val cname = info(1)
      (ename, cname)
    })
    val nationDataFrame = nationInfoDataSet.toDF("ename", "cname")

    //方式一：使用DSL风格
//    val joined = userDataFrame.join(nationDataFrame, $"nation" === $"ename")

    //方式二：使用SQL风格
    nationDataFrame.createTempView("t_nation")
    userDataFrame.createTempView("t_user")
    val joined: DataFrame = sparkSession.sql("select * from t_user left join t_nation on t_user.nation = t_nation.ename")

    joined.show()

    sparkSession.close()
  }

}
