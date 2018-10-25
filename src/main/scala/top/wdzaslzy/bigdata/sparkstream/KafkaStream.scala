//package top.wdzaslzy.bigdata.sparkstream
//
//import kafka.serializer.StringDecoder
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.dstream.InputDStream
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
///**
//  * @author lizy
//  **/
//object KafkaStream {
//
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
//    val ssc: StreamingContext = new StreamingContext(conf, Seconds(1))
//
//    // Kafka 配置
//    val topics = Set("calllog")
//    val brokers = "192.168.232.131:9092,192.168.232.132:9092,192.168.232.133:9092"
//    val kafkaParams = Map[String, String](
//      "metadata.broker.list" -> brokers,
//      "serializer.class" -> "kafka.serializer.StringEncoder")
//
//    val linesStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
//
//    val reduced = linesStream.flatMap(_._2.split(" ")).map((_, 1)).reduceByKey(_ + _)
//
//    reduced.print()
//    ssc.start() // 开始计算
//    ssc.awaitTermination() // 等待计算被中断
//  }
//
//}
