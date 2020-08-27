package com.lw.flinksql

import com.lw.flink.SensorReading
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Kafka, OldCsv, Schema}


object SourceTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

//    val inputStream = env.socketTextStream("localhost", 7777)
    val filePath = "/Users/xyj/developer/idea_prj/flink-helloworld/src/main/sources/sensor.txt"

    // 创建老版本流查询环境
    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env,settings)

    // 从外部系统读取数据，再注册表
    // 1.1 连接到文件系统（csv）
    tableEnv
      .connect(new FileSystem().path(filePath))
        .withFormat(new OldCsv)   // 定义读取数据之后的格式方法
        .withSchema(new Schema()
          .field("id",DataTypes.STRING())
          .field("timestamp",DataTypes.BIGINT())
          .field("temperature",DataTypes.DOUBLE())) //定义表结构
        .createTemporaryTable("file_table") //注册临时表


    // 1.2 连接到kafka
    tableEnv
      .connect(new Kafka()
        .version("0.11")
        .topic("kafka_topic_input")
        .property("bootstrap.servers","localhost:9092")
        .property("zookeeper.connect","localhost:2181"))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id",DataTypes.STRING())
        .field("timestamp",DataTypes.BIGINT())
        .field("temperature",DataTypes.DOUBLE()))
      .createTemporaryTable("kafka_table")

    // sensorTable 从环境中获取，主要用来做table api的
    val sensorTable: Table = tableEnv.from("file_table")

//    val resultSqlTable = tableEnv.sqlQuery("select id,count(1) cnt from kafka_table group by id ")
    val resultSqlTable = tableEnv.sqlQuery("select * from kafka_table where id='sensor_7'  ")

    resultSqlTable.toAppendStream[(String,Long,Double)].print("kafka --> table")


    env.execute("table test")

  }


}
