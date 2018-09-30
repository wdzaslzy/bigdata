package top.wdzaslzy.scala.`object`

/**
  * @author lizy
  **/
class ClassB(self: ClassA) {
  def b(): Unit = {
    self.a()
  }
}
