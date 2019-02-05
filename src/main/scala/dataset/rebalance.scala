package dataset

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, GroupedDataSet}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.functions.RichMapFunction

object rebalance {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds: DataSet[Long] = env.generateSequence(1, 3000)
    val data: DataSet[Long] = ds.filter(_ > 800)
    //遍历分区,均分
    val rebalanceData: DataSet[Long] = data.rebalance()
    val result: DataSet[(Int, Long)] = rebalanceData.map(new RichMapFunction[Long, (Int, Long)] {
      def map(in: Long) = {
        (getRuntimeContext.getIndexOfThisSubtask, in)
      }
    })
    result.print()


  }
}
