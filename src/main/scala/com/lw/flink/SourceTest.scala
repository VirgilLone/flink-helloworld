package com.lw.flink

import java.util.{Properties, Random}

import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011



case class SensorReading(id: String, timestamp: Long, temperature: Double)

object SourceTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //    env.setParallelism(5)

    /*// 集合读取数据
    val listStream = env.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.80018327300259),
      SensorReading("sensor_6", 1547718201, 15.402984393403084),
      SensorReading("sensor_7", 1547718202, 6.720945201171228),
      SensorReading("sensor_10", 1547718205, 38.101067604893444)
    ))
    listStream.print("listStream").setParallelism(1)*/


    /*// 从文件读取数据
    val txtStream = env.readTextFile("/Users/xyj/developer/idea_prj/flink-helloworld/src/main/sources/sensor.txt")
    txtStream.print("txtStream").setParallelism(1)
    // sink 到文件
    txtStream.addSink(StreamingFileSink.forRowFormat(
      new Path("/Users/xyj/developer/idea_prj/flink-helloworld/src/main/sources/out.txt"),
      new SimpleStringEncoder[String]("UTF-8")
    ).build())*/


    /*// kafka中读取
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    val kafka_stream = env.addSource(new FlinkKafkaConsumer011[String]("kafka_topic_log", new SimpleStringSchema(),properties))
    kafka_stream.print("kafka_stream").setParallelism(1)*/


    // 自定义输入源，需要继承 SourceFunction
    val diy_source = env.addSource(new MySensorSource())
    diy_source.print("diy_source").setParallelism(1)
    


    env.execute("source test")

  }

}

class MySensorSource extends SourceFunction[SensorReading]{

  // flag: 表示数据源是否还在正常运行
  var running: Boolean = true

  override def cancel(): Unit = {
    running = false
  }

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    // 初始化一个随机数发生器
    val rand = new Random()
    var curTemp = 1.to(10).map(
      i => ( "sensor_" + i, 65 + rand.nextGaussian() * 10 )
    )

    while(running){
      // 更新温度值
      curTemp = curTemp.map(
        t => (t._1, t._2 + rand.nextGaussian() )
      )
      // 获取当前时间戳
      val curTime = System.currentTimeMillis()
      curTemp.foreach(
        // 发送到流数据源 一条条发送
        t => sourceContext.collect(SensorReading(t._1, curTime, t._2))
      )
      Thread.sleep(100)
    }

  }

}
