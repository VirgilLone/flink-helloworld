import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._


object WorldCount_flink_stream {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(5)

    val params = ParameterTool.fromArgs(args)
    val host = params.get("host")
    val port = params.getInt("port")


    val txtDataStream = env.socketTextStream(host,port)


    val wcDataStream = txtDataStream.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map( (_,1) )
      .keyBy(0)
      .sum(1)

    wcDataStream.print()

    env.execute("wc_flink_job")

  }

}
