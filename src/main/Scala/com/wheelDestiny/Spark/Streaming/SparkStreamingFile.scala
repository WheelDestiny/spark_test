package com.wheelDestiny.Spark.Streaming

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

object SparkStreamingFile {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkStreamingFile").setMaster("local[*]")
    //设置读取多长时间范围内的文件
    conf.set("spark.streaming.fileStream.minRememberDuration", "2592000s")

    //每5s检查一次指定的目录
    val streamingContext = new StreamingContext(conf, Durations.seconds(5))
    val localPath = "D://input"

    val hadoopConf = new Configuration()

    val fileDStream: InputDStream[(LongWritable, Text)] = streamingContext.
      fileStream[LongWritable, Text, TextInputFormat](localPath,
      //设置文件过滤条件
      (path: Path) => {
        println(path)
        path.getName.endsWith(".txt")
      }, false, hadoopConf)

    val flatMap: DStream[String] = fileDStream.flatMap((f)=>{
      f._2.toString.split(" ")})


    val map: DStream[(String, Int)] = flatMap.map((_, 1))
    val value: DStream[(String, Int)] = map.reduceByKey(_ + _)

    value.foreachRDD(
      (values, time) => {
        println(s"count time:${time},${values.collect().toList}")
      }
    )

    streamingContext.start()
    streamingContext.awaitTermination()

  }

}
