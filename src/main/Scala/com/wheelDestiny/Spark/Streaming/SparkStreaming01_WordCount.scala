package com.wheelDestiny.Spark.Streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming01_WordCount {
  def main(args: Array[String]): Unit = {
    //1，初始化Spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamWordCount")
    //2，初始化SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    //3，通过监控端口创建DStream，读进来的数据以行为单位
    val lineStream: ReceiverInputDStream[String] = ssc.socketTextStream("op.hadoop", 9999)

    val wordStream: DStream[String] = lineStream.flatMap(_.split(" "))

    val wordCountStreams: DStream[(String, Int)] = wordStream.map((_, 1)).reduceByKey(_ + _)

    wordCountStreams.print()

    //启动SparkStreamContext
    ssc.start()
    ssc.awaitTermination()


  }

}
