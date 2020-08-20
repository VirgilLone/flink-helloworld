import org.apache.flink.api.scala._

object WorldCount_flink {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val inputPath = "/Users/xyj/developer/idea_prj/flink-helloworld/src/main/sources/wc.txt"
    val inputDataSet: DataSet[String] = env.readTextFile(inputPath)

    val wcDataSet = inputDataSet.flatMap(_.split(" "))
      .map( (_,1) )
      .groupBy(0)
      .sum(1)

    wcDataSet.print()

  }

}
