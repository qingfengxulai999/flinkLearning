package dataset

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable

object partition {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val data = new mutable.MutableList[(Int, Long, String)]
    data += ((1, 1L, "Hi1"))
    data += ((2, 1L, "Hi2"))
    data += ((3, 2L, "Hi3"))
    data += ((4, 2L, "Hi4"))
    data += ((5, 3L, "Hi5"))
    data += ((6, 3L, "Hi6"))
    data += ((7, 4L, "Hi7"))
    data += ((8, 4L, "Hi8"))
    data += ((9, 5L, "Hi9"))
    data += ((10, 5L, "Hi10"))
    data += ((11, 6L, "Hi11"))
    val ds: DataSet[(Int, Long, String)] = env.fromCollection(data)
    //按照hash进行分区,这里用的是第二个字段的值
    //val partitionByHash: DataSet[(Int, Long, String)] = ds.partitionByHash(1)
    //partitionByHash.writeAsText("partitionByHash",WriteMode.OVERWRITE)
    //按照范围进行分区,这里用的是第二个字段的值
    //val partitionByRange: DataSet[(Int, Long, String)] = ds.partitionByRange(1)
    //partitionByRange.writeAsText("partitionByRange",WriteMode.OVERWRITE)
    //设置分区数
    val parallelism: DataSet[(Int, Long, String)] = ds.setParallelism(2)
    val sortPartition: DataSet[(Int, Long, String)] = parallelism.sortPartition(2, Order.DESCENDING)
    sortPartition.writeAsText("sortPartition", WriteMode.OVERWRITE)
    env.execute()
  }
}
