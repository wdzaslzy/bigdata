package top.wdzaslzy.scala.base

/**
  * @author lizy
  **/
object ImplicitDemo {


  def main(args: Array[String]): Unit = {
    /*
    println("120"/ 12)
    正常情况，这句话编译时会报错，但是定义了隐式函数，将其导入，即可转换为想要的。
     */
//    import ImplicitDemo.StringToInt
//    println("120"/ 12)

    /*
    上述表示方式，等价于：
    println(StringToInt("120")/ 12)
     */
  }

}
