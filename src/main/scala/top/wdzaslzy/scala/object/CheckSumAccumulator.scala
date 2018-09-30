package top.wdzaslzy.scala.`object`

/**
  * @author lizy
  **/
class CheckSumAccumulator {

  //定义一个私有变量
  private var sum = 0;

  /*
    定义一个add方法
    参数为一个Int类型的值
    返回为Unit。即无返回
   */
  def add(value: Int): Unit = {
    sum += value
  }

  /*
    定义一个checkSum的方法
    无参数
    返回Int
   */
  def checkSum(): Int = sum

}
