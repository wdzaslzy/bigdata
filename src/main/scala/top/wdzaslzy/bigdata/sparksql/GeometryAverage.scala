package top.wdzaslzy.bigdata.sparksql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * @author lizy
  **/
object GeometryAverage {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("GeometryAverage").master("local[4]").getOrCreate()

    val dataSet = sparkSession.range(1, 10)
    val gen = new GenMean

    //方式一
//    dataSet.createTempView("t_range")
//    sparkSession.udf.register("gm", gen)
//    val frame = sparkSession.sql("select gm(id) as s from t_range")
//    frame.show()

    //方式二
    import sparkSession.implicits._
    dataSet.agg(gen($"id").as("s")).show()

    sparkSession.close()
  }
}

//计算n个数的乘积，自定义聚合函数，类似与SQL中的sum，avg等
class GenMean extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(List(StructField("value", DoubleType)))

  override def bufferSchema: StructType = StructType(List(
    //具体操作返回的结果
    StructField("product", DoubleType),
    //一共参与运算数量
    StructField("counts", LongType)))

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 1.0
    buffer(1) = 0L
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getDouble(0) * input.getDouble(0)
    buffer(1) = buffer.getLong(1) + 1
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getDouble(0) * buffer2.getDouble(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  override def evaluate(buffer: Row): Any = {
    Math.pow(buffer.getDouble(0), 1.0 / buffer.getLong(1))
  }
}
