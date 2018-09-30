package top.wdzaslzy.scala.base

/**
  * @author lizy
  **/
object Tuples {

  def main(args: Array[String]): Unit = {
    val tuple: (String, String) = ("a", "apple")
    println(tuple._1 + "==" + tuple._2)

    val tuple2 = ("a", "b", "c", 1, 2, 3)
    println(tuple2._4)
  }

}
