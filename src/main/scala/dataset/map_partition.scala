package dataset

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.scala._


object map_partition {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val data: DataSet[String] = env.fromElements("java", "spark", "flink")
    //比spark的mapPartition方便很多
    val partition_data: DataSet[String] = data.mapPartition(line => line.map(x => x + "1111"))
    partition_data.print()


  }
}
