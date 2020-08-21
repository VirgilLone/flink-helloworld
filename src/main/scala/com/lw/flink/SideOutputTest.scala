package com.lw.flink

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object SideOutputTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)


    val txtStream = env.socketTextStream("localhost", 7777)

    val dataStream = txtStream.map((e: String) => {
      val dataArrary = e.split(",")
      SensorReading(dataArrary(0).trim, dataArrary(1).trim.toLong, dataArrary(2).trim.toDouble)
    })
      //      .assignAscendingTimestamps(_.timestamp*1000)
      // 延迟1s上涨水位
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(t: SensorReading) = t.timestamp * 1000L
      })

    val processStream = dataStream.process(new FreezingMonitor())


    processStream.print("主流：")
    processStream.getSideOutput( OutputTag[String]("freezing-alarms")).print("测输出流：")

    env.execute("ProcessFunction test")

  }

}


class FreezingMonitor extends ProcessFunction[SensorReading, SensorReading] {
  // 定义一个侧输出标签
  lazy val freezingAlarmOutput: OutputTag[String] =
     OutputTag[String]("freezing-alarms")

  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
    // 如果传感器温度小于华氏32度，则输出报警信息到测输出流
    if (value.temperature < 32.0) {
      ctx.output(freezingAlarmOutput,"Freezing Alarm for "+value.id)
    } else {
      out.collect(value)
    }
  }
}
