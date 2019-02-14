package dataset

import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable

/**
  * 求极值的函数
  * aggregate
  * minBy & maxBy
  */
object most_values {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val data = new mutable.MutableList[(Int, String, Double)]
    data += ((1, "yuwen", 89.0))
    data += ((2, "shuxue", 88.0))
    data += ((3, "yingyu", 99.0))
    data += ((4, "huaxue", 93.0))
    data += ((1, "yuwen", 97.0))
    data += ((1, "shuxue", 88.0))
    data += ((1, "yingyu", 92.0))
    val input: DataSet[(Int, String, Double)] = env.fromCollection(data)
    //分组求top
    val result: AggregateDataSet[(Int, String, Double)] = input.groupBy(1).aggregate(Aggregations.MAX, 2)
    result.print()
    //可以指定多列排序
    input.minBy(2, 0).print()


  }

}
