package flink.demo

import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment, GroupedDataSet}
import org.apache.flink.streaming.api.scala._

class demo1_WORDCOUNT {
}

object demo1_WORDCOUNT{
  def main(args: Array[String]): Unit = {
    //1.获取一个execution environment
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //2.加载/创建初始数据
    val text: DataSet[String] = env.fromElements(
      "Who's there?",
      "I think I hear them. Stand, ho! Who's there?")
    //3.指定这些数据的转换
    val flatmap_data: DataSet[String] = text.flatMap(line=>line.toLowerCase().split("\\W+"))
    val nonEmpty_data: DataSet[String] = flatmap_data.filter(line=>line.nonEmpty)
    val tuple_data: DataSet[(String, Int)] = nonEmpty_data.map(line=>(line,1))
    val group_data: GroupedDataSet[(String, Int)] = tuple_data.groupBy(0)
    val total_data: AggregateDataSet[(String, Int)] = group_data.sum(1)
    //total_data.print()
    //4.保存地址
    total_data.writeAsText("hdfs://node1:8020/wordcount")
    //5.触发程序执行
    env.execute()



  }

}
