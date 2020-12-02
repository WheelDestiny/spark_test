package com.wheelDestiny.Spark.Streaming

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

object SparkStreamingSocketFileCogroup {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkStreamingSocketFileCogroup").setMaster("local[*]")
    conf.set("spark.streaming.fileStream.minRememberDuration", "6000000s")
    //每5s检查一次指定的目录
    val streamingContext = new StreamingContext(conf, Durations.seconds(10))
    val localPath = "checkDir"

    val lines: ReceiverInputDStream[String] = streamingContext.socketTextStream("op.hadoop", 7654)

    val countryCount: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    val configuration = new Configuration()

    val fileStream: InputDStream[(LongWritable, Text)] = streamingContext.fileStream[LongWritable, Text, TextInputFormat](localPath,
      (path: Path) => {
        println(path)
        path.getName.endsWith(".conf")
      }, false, configuration
    )
    val countryDirt: DStream[(String, String)] = fileStream.map((a) => {
      val strings: Array[String] = a._2.toString.split(" ")
      (strings(0), strings(1))
    })

    countryDirt.cogroup(countryCount).foreachRDD(
      (res,time)=>{
        println(s"count time:${time}")
        res.foreach(i=>{
          println(s"countryCode:${i._1},countryName:${i._2._1},countryCount:${i._2._2}")
        })
      }
    )
    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
