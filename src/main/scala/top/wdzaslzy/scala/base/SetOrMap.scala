package top.wdzaslzy.scala.base

/**
  * @author lizy
  **/
object SetOrMap {

  def main(args: Array[String]): Unit = {
    var set = Set("hello", "word")
    set += "java"
    println(set)

    var set2 = scala.collection.mutable.Set("hello", "word")
    set2.add("scala")
    println(set2)

    val map = Map(1 -> "I", 2 -> "II")
    println(map.get(2))
  }
}
