package top.wdzaslzy.scala.base

import scala.io.Source

/**
  * @author lizy
  **/
object Test {

  def main(args: Array[String]): Unit = {
    val source = Source.fromFile("E:\\IPLog.txt")
    source.getLines().foreach(println(_))
  }

}
