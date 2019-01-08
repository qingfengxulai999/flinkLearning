package dataset

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, GroupedDataSet}
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

/**
  * 针对于大数据量时,reduceGroup优于reduce操作,
  * reduce操作是将所有数据都拉取之后再做处理,而reduceGroup操作是对于每个组内数据先进行处理后再拉取,可以减少网络io
  */
object reduceGroup {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val data: DataSet[List[(String, Int)]] = env.fromElements(List(("java",1),("hadoop",1),("java",1)))
    val flatMap_data: DataSet[(String, Int)] = data.flatMap(line=>line)
    val groupBy_data: GroupedDataSet[(String, Int)] = flatMap_data.groupBy(line=>line._1)
    val result: DataSet[(String, Int)] = groupBy_data.reduceGroup(
      (input: Iterator[(String, Int)], output: Collector[(String, Int)]) => {
        val reduce_data: (String, Int) = input.reduce((x, y) => (x._1, x._2 + y._2))
        output.collect(reduce_data)
      }
    )
    result.print()





  }

}
