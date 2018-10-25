package top.wdzaslzy.bigdata.sparkstream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author lizy
  **/
object SparkStreamDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))

    //从socket读数据
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.232.131", 8888)
    val words: DStream[String] = lines.flatMap(_.split(","))
    val reduced: DStream[(String, Int)] = words.map((_, 1)).reduceByKey(_ + _)

    reduced.print()

    ssc.start()

    ssc.awaitTermination()
  }

}
