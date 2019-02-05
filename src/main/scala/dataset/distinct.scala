package dataset

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

import scala.collection.mutable
import scala.util.Random
import org.apache.flink.streaming.api.scala._

object distinct {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val data = new mutable.MutableList[(Int,String,Double)]
    data+=((1,"yuwen",89.0))
    data+=((2,"shuxue",88.0))
    data+=((3,"yingyu",99.0))
    data+=((4,"huaxue",93.0))
    data+=((1,"yuwen",97.0))
    data+=((1,"shuxue",88.0))
    data+=((1,"yingyu",92.0))
    val input: DataSet[(Int, String, Double)] = env.fromCollection(Random.shuffle(data))
    input.distinct(1).print()





  }

}
