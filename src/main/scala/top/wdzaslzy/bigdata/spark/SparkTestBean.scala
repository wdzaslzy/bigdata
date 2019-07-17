package top.wdzaslzy.bigdata.spark

import scala.reflect.ClassTag

/**
  * @author zhongyou_li
  */
class SparkTestBean[T] extends Serializable {

  var name: String = _
  var age: String = _

  def test[T: ClassTag](arr: Array[Array[T]]): Unit = {

    val flatten: Array[T] = arr.flatten
    println(flatten.length)

  }

}