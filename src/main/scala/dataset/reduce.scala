package dataset

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, GroupedDataSet}
import org.apache.flink.streaming.api.scala._

object reduce {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val data: DataSet[List[(String, Int)]] = env.fromElements(List(("java",1),("spark",1),("java",1)))
    val flatMap_data: DataSet[(String, Int)] = data.flatMap(line=>line)
    val groupBy_data: GroupedDataSet[(String, Int)] = flatMap_data.groupBy(0)
    val result: DataSet[(String, Int)] = groupBy_data.reduce((x,y)=>(x._1,(x._2+y._2)))
    result.print()

  }
}
