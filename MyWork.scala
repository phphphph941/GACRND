package com.oracle.analyse.session


import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object MyWork {
  def main(args: Array[String]): Unit = {
    //1.创建 SparkConf
    val sparkConf: SparkConf = new
        SparkConf().setAppName("ReceiverWordCount").setMaster("local[*]")
    //2.创建 StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    //3.定义 Kafka 参数
    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG ->
        "master:9092,slave1:9092,slave2:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "test",
      "key.deserializer" ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" ->
        "org.apache.kafka.common.serialization.StringDeserializer"
    )
    //4.读取 Kafka 数据创建 DStream
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream[String, String](ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](Set("callslog"), kafkaPara))
    //5.将每条消息的 KV 取出
    val valueDStream: DStream[String] = kafkaDStream.map(record => record.value())
    //                通话编号 主叫人所在城市 主叫人网络类型(2G|3G|4G|5G) 主叫人移动卡业务类型(全球通|神州行|动感地带) 主叫号码 主叫人姓名 被叫人所在城市 被叫人网络类型(2G|3G|4G|5G) 被叫人运营商类型(移动|联通|电信) 被叫号码 被叫人姓名 通话开始时间 通话时长 通话类型(接通|未接通|语音信箱)
//    1554475c1b6d44a4887e7f184480f5fe 441226 4G Easyown 13569728258 邹示 652327 4G ChinaUnicom 13101667484 武星 2022-08-30_09:33:35 0487 CONNECT_CALL
//    valueDStream.foreachRDD(rdd => rdd.foreach(println))

    sparkConf.set("spark.sql.warehouse.dir", "/user/hive/warehouse")
    sparkConf.set("hive.metastore.uris", "thrift://master:9083")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()



    val callRecords: DStream[Array[String]] = valueDStream.map(_.split(" "))
//
//
    // 定义数据结构的模式
    val schema = StructType(Seq(
      StructField("CallerNetworkType", StringType),
      StructField("Count", StringType)
    ))



    // 统计主叫人网络类型的通话总数
    callRecords
      .map(fields => (fields(2), 1))
      .reduceByKey(_ + _)
      .foreachRDD { rdd =>
        if (!rdd.isEmpty()) {
          val result = rdd.map { case (networkType, count) => Row(networkType, count.toString) }
          val callerNetworkCount = sparkSession.createDataFrame(result, schema)
          callerNetworkCount.write.mode("append").saveAsTable("default,callerNetType")
        }
      }

    // 统计被叫人运营商的通话总数
    callRecords
      .map(fields => (fields(8), 1))
      .reduceByKey(_ + _)
      .foreachRDD { rdd =>
        if (!rdd.isEmpty()) {
          val result = rdd.map { case (operatorType, count) => Row(operatorType, count.toString) }
          val calleeOperatorCount = sparkSession.createDataFrame(result, schema)
          calleeOperatorCount.write.mode("append").saveAsTable("default.calleeServiceType")
        }
      }

    // 统计一个小时内通话类型为未接通（LOSS_CALL）的通话总数
    callRecords
      .filter(fields => fields(13) == "LOSS_CALL")
      .count()
      .foreachRDD { rdd =>
        if (!rdd.isEmpty()) {
          val lossCallCount = rdd.collect().head
          val resultDF = sparkSession.createDataFrame(Seq(("Loss Call Count", lossCallCount)))
            .toDF("Metric", "Count")
          resultDF.write.mode("append").saveAsTable("default.lossCallCountByHour")
        }
      }






    //    //7.开启任务
    ssc.start()
    ssc.awaitTermination()
  }

}


/*
* CREATE TABLE IF NOT EXISTS default.callerNetType (
  networkType STRING,
  count STRING
)
CREATE TABLE IF NOT EXISTS default.calleeServiceType (
  operatorType STRING,
  count STRING
)

*
* CREATE TABLE IF NOT EXISTS default.lossCallCountByHour (
  Metric STRING,
  Count LONG
)

*
*
*  */
