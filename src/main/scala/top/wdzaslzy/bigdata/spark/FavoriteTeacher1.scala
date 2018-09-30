package top.wdzaslzy.bigdata.spark

import org.apache.spark.sql.SparkSession

/**
  * @author lizy
  **/
object FavoriteTeacher1 {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("FavoriteTeacher1")
      .master("local[4]")
      .getOrCreate()

    //依次读入数据，每次都1行，分3个分区
    val lineRdd = sparkSession.sparkContext.textFile("E:\\teacher.log", 3)
    /*
      map方法中的方法，类似与Java中的接口中的一个方法，在执行map时，进行回调
     */
    val teacherRdd = lineRdd.map(line => splitTeacherForLine(line))
    val teacherMapRdd = teacherRdd.map(teacher => (teacher, 1))
    val reducedRdd = teacherMapRdd.reduceByKey(_ + _)
    val sortedRdd = reducedRdd.sortBy(tuple => tuple._2, ascending = false)
    val result = sortedRdd.collect()
    println("输出结果为：" + result.toBuffer)

    sparkSession.close()
  }

  def splitTeacherForLine(line: String): String = {
    val words = line.split("/")
    words(words.length - 1)
  }

}

/*
解析该过程的执行
3个分区，每个阶段3个task
transformation
第一阶段：RDD转换
task1     读取文件 1-10行    分割后得到老师姓名   在这个分区进行map
task2     读取文件11-20行    分割后得到老师姓名   在这个分区进行map
task3     读取文件21-30行    分割后得到老师姓名   在这个分区进行map

第二阶段：reduce阶段
调用reduceByKey，进行聚合，聚合将全部分区中相同的key再进行聚合
聚合之后sortByKey，先在每个分区进行排序

第三阶段：collect阶段
此阶段将全局进行shuffle和sort
 */
