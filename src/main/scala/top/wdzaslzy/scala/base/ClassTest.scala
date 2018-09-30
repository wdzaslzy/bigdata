package top.wdzaslzy.scala.base

import scala.collection.mutable

/**
  * @author lizy
  **/
object ClassTest {

  def main(args: Array[String]): Unit = {
    val map = new mutable.HashMap[String, Int]()
    map.put("java", 2)
    map.put("php", 3)

    println(map("bigdata"))
  }

}
