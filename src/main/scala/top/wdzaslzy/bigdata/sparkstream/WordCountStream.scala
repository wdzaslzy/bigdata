package top.wdzaslzy.bigdata.sparkstream

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf}

/**
  * @author lizy
  **/
object WordCountStream {

  /*
  记住状态，实现累加
   */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("WordCount")

    val streamingContext = new StreamingContext(conf, Seconds(2))
    streamingContext.checkpoint("E:\\point")

    val lines = streamingContext.socketTextStream("192.168.232.131", 8888)
    val reduced = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)


    val updateFunc = (its: Iterator[(String, Seq[Int], Option[Int])]) => {
      its.map(it => {
        (it._1, it._2.sum + it._3.getOrElse(0))
      })
    }
    val result = reduced.updateStateByKey(updateFunc, new HashPartitioner(2), rememberPartitioner = true)

    result.print()

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
