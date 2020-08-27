package com.lw.flinksql

import com.lw.flink.SensorReading
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._

/**
 * DataStream流转化为表
 */
object TableTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


//    val inputStream = env.socketTextStream("localhost", 7777)
    val inputPath = "/Users/xyj/developer/idea_prj/flink-helloworld/src/main/sources/sensor.txt"
    val inputStream: DataStream[String] = env.readTextFile(inputPath)

    val dataStream = inputStream.map((e: String) => {
      val dataArray = e.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })

    // 1.1 创建老版本流查询环境
    val settings = EnvironmentSettings.newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env,settings)

    // 1.2 创建老版本批式查询环境
    val batchTableEnvironment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val batchTableEnv = BatchTableEnvironment.create(batchTableEnvironment)

    // 1.3 创建blink版本的流查询环境
    val bsSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val bsTableEnv = StreamTableEnvironment.create(env,bsSettings)

    // 1.4 创建blink的批式查询环境
    val bbSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val bbTableEnv = StreamTableEnvironment.create(env,bbSettings)


    // 基于tableEnv，流转化为Table，但是Table还不能直接在sql里使用必须要注册临时表
    val dataTable: Table = bsTableEnv.fromDataStream(dataStream,
      'timestamp as 'ts,'id as 'id,'temperature)
//    bsTableEnv.registerTable("dataTable",dataTable)
//    bsTableEnv.createTemporaryView("dataTable",dataTable)

    // 可以直接把流注册为临时表供flinksql使用，少了中间的Table数据结构
    bsTableEnv.createTemporaryView("dataTable",dataStream,
      'timestamp as 'ts,'id as 'id,'temperature)

    // 使用Table api
//    val resultSqlTable: Table = dataTable
//      .select("id,temperature")
//      .filter("id == 'sensor_7' ")

    // 或者使用flinksql 直接写sql
    val resultSqlTable = bsTableEnv.sqlQuery("select ts,id,temperature from dataTable where id='sensor_7'  ")
//    val resultSqlTable = bsTableEnv.sqlQuery("select id,temperature from "+ dataTable+" where id='sensor_7'  ")

    resultSqlTable.toAppendStream[(Long,String, Double)].print("table api")

    env.execute("table test")

  }


}
