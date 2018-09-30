package top.wdzaslzy.scala.base

/**
  * @author lizy
  **/
object Lists {

  def main(args: Array[String]): Unit = {
    val list1 = List[Int](1, 2)

    val list2 = List[String]("3", "4")

    val list3: List[Any] = list1 ::: list2

    println(list3)      //List(1, 2, 3, 4)

    val list4 = list1 :: 5 :: Nil
    println(list4)      //List(List(1, 2), 5)

  }

}
