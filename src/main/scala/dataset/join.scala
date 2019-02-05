package dataset
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, GroupedDataSet}

import scala.collection.mutable
import scala.util.Random
import org.apache.flink.streaming.api.scala._

object join {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //joinList1:学号,学科,成绩
    val data = new mutable.MutableList[(Int,String,Double)]
    data+=((1,"yuwen",89.0))
    data+=((2,"shuxue",88.0))
    data+=((3,"yingyu",99.0))
    data+=((4,"huaxue",93.0))
    data+=((5,"yuwen",97.0))
    data+=((6,"yuwen",88.0))
    data+=((7,"yingyu",92.0))
    //joinList2:学号,班级
    val data2 = new mutable.MutableList[(Int,String)]
    data2+=((1,"class1"))
    data2+=((2,"class1"))
    data2+=((3,"class2"))
    data2+=((4,"class2"))
    data2+=((5,"class3"))
    data2+=((6,"class3"))
    data2+=((7,"class4"))
    val input1: DataSet[(Int, String, Double)] = env.fromCollection(Random.shuffle(data))
    val input2: DataSet[(Int, String)] = env.fromCollection(Random.shuffle(data2))
    val result: DataSet[(Int, String, Double, String)] = input1.join(input2).where(0).equalTo(0) {
      (line1, line2) => (line1._1, line1._2, line1._3, line2._2)
    }
    val groupByResult: GroupedDataSet[(Int, String, Double, String)] = result.groupBy(1,3)
    groupByResult.aggregate(Aggregations.MAX,2).print()










  }

}
