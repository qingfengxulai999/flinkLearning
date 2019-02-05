package dataset

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, GroupedDataSet}
import org.apache.flink.streaming.api.scala._

object union {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ele1: DataSet[String] = env.fromElements("123")
    val ele2: DataSet[String] = env.fromElements("456")
    val ele3: DataSet[String] = env.fromElements("123")
    val unionResult: DataSet[String] = ele1.union(ele2).union(ele3)
    unionResult.distinct.print
  }
}
