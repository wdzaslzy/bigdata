package top.wdzaslzy.scala.base

/**
  * @author lizy
  **/
object Test {

  def main(args: Array[String]): Unit = {
    val s = "http://bigdata.edu360.cn/laozhao"
    val words = s.split("/")
//    words.foreach(word => println(word))

    println(words(2).split("\\.")(0))
  }

}
