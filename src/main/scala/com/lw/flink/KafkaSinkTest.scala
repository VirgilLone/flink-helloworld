package com.lw.flink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

object KafkaSinkTest {

  def main(args: Array[String]): Unit = {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //    env.setParallelism(5)

    // kafka中读取
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val kafka_input_stream = env.addSource(new FlinkKafkaConsumer011[String]("kafka_topic_input", new SimpleStringSchema(), properties))
    kafka_input_stream.print("kafka_input_stream").setParallelism(1)


    val dataStream = kafka_input_stream.map((data: String) =>{
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble).toString
    })
//    val dataStream2 = dataStream.filter(e =>e.id.nonEmpty)
//      .keyBy(0)
//        .reduce( (x,y)=> SensorReading(x.id, y.timestamp-1, y.temperature+10) )

    // sink
    dataStream.addSink(new FlinkKafkaProducer011[String]("kafka_sink_topic_output", new SimpleStringSchema(), properties))

    dataStream.print("kafka_sink_topic_output------>")

    env.execute("kafka sink test")

  }


}
