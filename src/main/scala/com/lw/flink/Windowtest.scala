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
    // 设置周期性插入水位线的时间 flink默认是200ms
//    env.getConfig.setAutoWatermarkInterval(300L)


    val txtStream = env.socketTextStream("localhost", 7777)

    val dataStream = txtStream.map((e: String) => {
      val dataArrary = e.split(",")
      SensorReading(dataArrary(0).trim, dataArrary(1).trim.toLong, dataArrary(2).trim.toDouble)
    })
      //      .assignAscendingTimestamps(_.timestamp*1000)
      // 设置延迟时间 1秒  最好等于最大乱序时间   watermark=当前最大时间戳-延迟时间,
      // 使得watermark不断后移，直到触发一个窗口关闭的时间
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(t: SensorReading) = t.timestamp * 1000L
      })


    val maxTemp = dataStream.map(e => (e.id, e.temperature))
      .keyBy(_._1)
//      .window(SlidingEventTimeWindows.of(Time.seconds(15),Time.seconds(15),Time.hours(-8)))
      // assigner 决定数据去哪个窗口
      .timeWindow(Time.seconds(5))
      // 设置窗口允许处理迟到数据为1分钟，迟到数据放入原来本该关闭的窗口再和本窗口已经输出的结果做聚合
      .allowedLateness(Time.minutes(1))
      // 设置迟到的数据写入侧输出流
      .sideOutputLateData(new OutputTag[(String,Double)]("lated"))
      // windowFunction 窗口关闭时做的操作
      .reduce((data1, data2) => (data1._1, data1._2.min(data2._2)))

    txtStream.print("source-->")
    maxTemp.print("min input")

    maxTemp.getSideOutput(new OutputTag[(String,Double)]("lated")).print("side output data")

    env.execute("window test")

  }

}
