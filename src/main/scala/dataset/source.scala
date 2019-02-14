package dataset

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

object source {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    /*
    //1.读取本地text文件
    val data: DataSet[String] = env.readTextFile("data/words.txt")
    //这里的"\\W+"匹配的是任意的非单词字符
    data.flatMap(_.toLowerCase.split("\\W+"))
      .map(line=>(line,1))
      .groupBy(0)
      .reduce((x,y)=>(x._1,x._2+y._2))
      .print()
    */
    //2.读取本地csv文件  需要在方法后面[]里加上返回值的类型
    /*
    val data: DataSet[(Int, String, String, String)] = env.readCsvFile[(Int, String, String, String)](
      filePath = "data/words.csv",
      lineDelimiter = "\n",
      fieldDelimiter = ",",
      lenient = false, //容错处理
      ignoreFirstLine = true,
      includedFields = Array(0, 1, 2, 3))
    data.print()
    */
    //3.递归读取,需要设置参数
    val param = new Configuration
    param.setBoolean("recursive.file.enumeration", true)
    val data: DataSet[String] = env.readTextFile("data/digui").withParameters(param)
    data.map(_.split(" ")).map(line=>line(0)+"zui"+line(1)+line(2)+"!").print()
  }
}
