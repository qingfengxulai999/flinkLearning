package dataset

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
object map_flatmap {
        def main(args: Array[String]): Unit = {
          val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
          val data: DataSet[(String, Int)] = env.fromElements(("A",1),("B",1),("C",1))
          val map_result: DataSet[String] = data.map(line=>line._1+line._2)
          map_result.print()
          //todo:这里为什么?
          val flatMap_result: DataSet[Char] = data.flatMap(line=>line._1+line._2)
          flatMap_result.print()










        }
}
