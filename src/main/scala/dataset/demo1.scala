package dataset

import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment, GroupedDataSet}
import org.apache.flink.streaming.api.scala._


object demo1 {
  def main(args: Array[String]): Unit = {
    /**
      * 统计相邻字符串出现的次数
      * A;B;C;D;B;D;C
      * B;D;A;E;D;C
      * A;B
      */
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val data: DataSet[String] = env.fromElements("A;B;C;D;B;D;C;B;D;A;E;D;C;A;B")
    val map_data: DataSet[Array[String]] = data.map(line => line.split(";"))
    val tuple_data: DataSet[(String, Int)] = map_data.flatMap {
      line => for (index <- 0 until line.length - 1) yield (line(index) + "+" + line(index + 1), 1)
    }
    val group_data: GroupedDataSet[(String, Int)] = tuple_data.groupBy(0)
    //等同于reduce((x,y)=>(x._1,x._2+y._2))
    val result: AggregateDataSet[(String, Int)] = group_data.sum(1)
    result.print()


  }
}
