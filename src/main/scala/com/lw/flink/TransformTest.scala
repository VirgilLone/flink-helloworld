package com.lw.flink

import org.apache.flink.streaming.api.scala._

object TransformTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val fileStream = env.readTextFile("/Users/xyj/developer/idea_prj/flink-helloworld/src/main/sources/sensor.txt")

    // 1。基本转换算子和简单聚合算子
    val dataStream:DataStream[SensorReading] = fileStream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    }
    )
    val aggStream = dataStream    .keyBy(0)
//        .sum(2)
        .reduce( (x,y)=> SensorReading(x.id, x.timestamp+1, y.temperature+10) )
    aggStream.print()

//    // 根据id合并，时间戳取最新的，温度取两者之和
//    val aa = dataStream.keyBy(0).reduce((x,y)=>SensorReading(x.id,y.timestamp,x.temperature+y.temperature))
//    aa.print()


    // 2。多流转换算子 split+select。
    val splitStream = dataStream.split(sensorData =>
      if (sensorData.temperature>30) Seq("high") else Seq("normal")
    )
    val high = splitStream.select("high")
    val normal = splitStream.select("normal")

    high.print("high")
    normal.print("normal")


    // 连接两个数据流 connect+coMap。      union可多个
    val warning = high.map( sensorData => (sensorData.id,
      sensorData.temperature) )
    val connected = warning.connect(normal)
    val coMap = connected.map(
      warningData => (warningData._1, warningData._2, "warning"),
      lowData => (lowData.id, "healthy")
    )

    coMap.print("combined")


    env.execute("transform test")
  }

}
