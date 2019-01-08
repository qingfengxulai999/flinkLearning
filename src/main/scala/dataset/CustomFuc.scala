package dataset

import java.lang

import org.apache.flink.api.common.functions.{GroupCombineFunction, GroupReduceFunction}
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, GroupedDataSet}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * 这里用自定义reduce函数代替了在main函数中直接书写逻辑,这样在main函数中大大减少了代码量
  * 为后续维护带来的便利,但是这里使用的reduceGroup容易产生oom的问题,因为需要一次性转化完毕
  * 还有一种combineGroup,它不容易产生oom,但是有可能得到不完整的结果
  */
object Custom_fuc {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val data: DataSet[List[(String, Int)]] = env.fromElements(List(("java", 1), ("hadoop", 1), ("java", 1)))
    val flatMap_data: DataSet[(String, Int)] = data.flatMap(line => line)
    val groupBy_data: GroupedDataSet[(String, Int)] = flatMap_data.groupBy(line => line._1)
    val group_data: DataSet[(String, Int)] = groupBy_data.reduceGroup(new Custom_fuct())
    group_data.print()
  }

}

import collection.JavaConverters._

class Custom_fuct extends GroupReduceFunction[(String, Int), (String, Int)] with GroupCombineFunction[(String, Int), (String, Int)] {
  override def reduce(values: lang.Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
    for (in <- values.asScala){
      out.collect(in._1,in._2)
    }
  }

  override def combine(values: lang.Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
    var num = 0
    var s = ""
    for (in <- values.asScala) {
      num += in._2
      s = in._1
    }
    out.collect((s, num))
  }
}
