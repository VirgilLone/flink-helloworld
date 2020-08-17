package com.lw.flink


import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object Windowtest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    // 使用event时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    val txtStream = env.socketTextStream("localhost", 7777)

    val dataStream = txtStream.map((e: String) => {
      val dataArrary = e.split(",")
      SensorReading(dataArrary(0).trim, dataArrary(1).trim.toLong, dataArrary(2).trim.toDouble)
    })
      //      .assignAscendingTimestamps(_.timestamp*1000)
      // 延迟1s上涨水位
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(t: SensorReading) = t.timestamp * 1000
      })


    val maxTemp = dataStream.map(e => (e.id, e.temperature))
      .keyBy(_._1)
//      .window(SlidingEventTimeWindows.of(Time.seconds(15),Time.seconds(15),Time.hours(-8)))
      .timeWindow(Time.seconds(5)) // assigner 决定数据去哪个窗口
      .reduce((data1, data2) => (data1._1, data1._2.min(data2._2))) // windowFunction 窗口关闭时做的操作

    txtStream.print("source-->")
    maxTemp.print("min input")

    env.execute("window test")

  }

}
