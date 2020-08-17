package com.lw.flink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object RedisSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //    env.setParallelism(5)

    val inputStream = env.readTextFile("/Users/xyj/developer/idea_prj/flink-helloworld/src/main/sources/sensor.txt")

    val dataStream: DataStream[SensorReading] = inputStream.map((data: String) => {
      val dataArray: Array[String] = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })


    // sink
    val conf = new
        FlinkJedisPoolConfig.Builder().setHost("localhost").setPort(6379).build()

    dataStream.addSink(new RedisSink[SensorReading](conf, new MyRedisMapper))


    dataStream.print("redis_sink_topic_output------>")

    env.execute("redis sink test")

  }

}
// 定义保存到 redis 时调用的命令：
class MyRedisMapper extends RedisMapper[SensorReading]{
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET, "sensor_temperature")
  }
  override def getKeyFromData(t: SensorReading): String = t.id
  override def getValueFromData(t: SensorReading): String = t.temperature.toString
}
