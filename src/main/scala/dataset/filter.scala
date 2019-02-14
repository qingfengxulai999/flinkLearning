package dataset

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.scala._


object filter {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val data: DataSet[String] = env.fromElements("java", "spark", "flink")
    val filter1: DataSet[String] = data.filter(line => line.contains("flink"))
    filter1.print()

  }

}
