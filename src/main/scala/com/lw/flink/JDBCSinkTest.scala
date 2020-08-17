package com.lw.flink

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

object JDBCSinkTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //    env.setParallelism(5)

    val inputStream = env.readTextFile("/Users/xyj/developer/idea_prj/flink-helloworld/src/main/sources/sensor.txt")

    val dataStream: DataStream[SensorReading] = inputStream.map((data: String) => {
      val dataArray: Array[String] = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })

    dataStream.print()
    dataStream.addSink( new MyJDBCSink2() )


    env.execute("jdbc sink test")

  }

}

// 不能来一条数据就建立一次连接，所有需要有数据库连接生命周期之类的支持，故引入RichFunction
class MyJDBCSink2() extends RichSinkFunction[SensorReading]{
  // 定义sql的连接、定义预编译器
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  // 初始化，创建连接和预编译语句
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
     conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test",
      "root", "root666")
    insertStmt = conn.prepareStatement("INSERT INTO sensor (sensor_id, temperature) VALUES (?, ?) ")
    updateStmt = conn.prepareStatement("UPDATE sensor SET temperature = ? WHERE sensor_id = ? ")


  }

  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
//    super.invoke(value, context)
    updateStmt.setDouble(1, value.temperature)
    updateStmt.setString(2, value.id)
    updateStmt.execute()
    // update没有成功，则需要执行insert
    if (updateStmt.getUpdateCount == 0) {
      insertStmt.setString(1, value.id)
      insertStmt.setDouble(2, value.temperature)
      insertStmt.execute()
    }
  }

  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }

}
