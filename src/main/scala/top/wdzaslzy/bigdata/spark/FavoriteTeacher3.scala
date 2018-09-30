package top.wdzaslzy.bigdata.spark

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  * @author lizy
  **/
object FavoriteTeacher3 {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("FavoriteTeacher1")
      .master("local[4]")
      .getOrCreate()

    //依次读入数据，每次都1行，分3个分区
    val lineRdd = sparkSession.sparkContext.textFile("E:\\teacher.log", 3)
    val relationRdd = lineRdd.map(line => {
      val words = line.split("/")
      val teacher = words(words.length - 1)
      val subject = words(2).split("\\.")(0)
      (subject, teacher)
    })
    val relationCountRdd = relationRdd.map(relation => (relation, 1))
    val subjects = relationRdd.map(_._1).distinct().collect()

    val partitionRdd: RDD[((String, String), Int)] = relationCountRdd.reduceByKey(new SubjectPartitioner(subjects), _ + _)
    //即排序，又不加载到内存中
    //TODO
    val sortedRdd = partitionRdd.mapPartitions(it => it.toList.sortBy(_._2).iterator)
    //收集结果
    val result = sortedRdd.collect()
    println("输出结果为：" + result.toBuffer)

    sparkSession.close()
  }
}

//自定义分区器
class SubjectPartitioner(subjects: Array[String]) extends Partitioner {

  private val ruleMap = new mutable.HashMap[String, Int]
  var index: Int = 0
  subjects.foreach(subject => {
    ruleMap.put(subject, index)
    index += index
  })

  override def numPartitions: Int = subjects.length

  override def getPartition(key: Any): Int = ruleMap(key.asInstanceOf[(String, String)]._1)
}

class FiveItemIterator {
  private val tuples = new Array[((String, String), Int)](5)



}
