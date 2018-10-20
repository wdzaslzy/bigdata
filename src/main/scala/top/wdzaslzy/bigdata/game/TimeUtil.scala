package top.wdzaslzy.bigdata.game

import java.text.SimpleDateFormat

/**
  * @author lizy
  **/
object TimeUtil {

  private val dateFormat = new SimpleDateFormat("yyyy年MM月dd日,E,HH:mm:ss")

  //将字符串的Time转换为Long
  def formatTimeToLong(time:String):Long = {
    dateFormat.parse(time).getTime
  }

}
