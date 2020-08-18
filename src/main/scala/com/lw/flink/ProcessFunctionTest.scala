package com.lw.flink

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * 需求：监控温度传感器的温度值，如果温度值在5秒钟之内(processing time)连
 * 续上升，则报警
 */
object ProcessFunctionTest {

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

    val processedStream = dataStream.keyBy(_.id)
      .process(new TempIncreAlert())

    dataStream.print("data stream ")
    processedStream.print("process stream")


    env.execute("ProcessFunction test")

  }

}

class TempIncreAlert extends KeyedProcessFunction[String, SensorReading, String] {
  // 定义一个状态，用来保存上一个传感器的温度值
  lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", Types.of[Double]))

  // 保存注册的定时器的时间戳
  lazy val currentTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer", Types.of[Long]))


  override def processElement(i: SensorReading, context: KeyedProcessFunction[String, SensorReading, String]#Context, collector: Collector[String]): Unit = {

    // 根据数据流入情况定义定时器，再在onTimer回调里定义需要做的处理

    // 取出上一次的温度
    val prevTemp = lastTemp.value()
    // 将当前温度更新到上一次的温度这个变量中
    lastTemp.update(i.temperature)

    val curTimerTs = currentTimer.value()
    if (i.temperature < prevTemp || prevTemp == 0.0 ) {
      // 温度下降或者是第一个温度值，删除定时器
      context.timerService().deleteProcessingTimeTimer(curTimerTs)
      // 清空状态变量
      currentTimer.clear()
    } else if (i.temperature > prevTemp && curTimerTs == 0) {
      // 温度上升且我们并没有设置定时器，就注册一个定时器，等一秒钟后触发，看这5秒内的数据
      val timerTs = context.timerService().currentProcessingTime() + 5000
      context.timerService().registerProcessingTimeTimer(timerTs)
      currentTimer.update(timerTs)
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
//    super.onTimer(timestamp, ctx, out)
    out.collect("警报警报！"+ctx.getCurrentKey+"温度连续上升！")
    currentTimer.clear()

  }



}
