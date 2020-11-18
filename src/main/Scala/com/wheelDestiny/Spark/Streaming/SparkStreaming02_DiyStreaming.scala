package com.wheelDestiny.Spark.Streaming

import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming02_DiyStreaming {
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
class CustomerReceiver(host:String,port:String) extends Receiver[String](StorageLevel.MEMORY_ONLY){
  //最初启动的时候，调用该方法，作用为：读取数据并发送给spark
  override def onStart(): Unit = {
    new Thread("Socket RReceiver"){
      override def run(): Unit = {

      }
    }.start()
  }
  def receive(): Unit ={
    //创建一个Socket
//    var socket: Socket = new Socket(host, port)
    var input:String = null

  }

  override def onStop(): Unit = {

  }
}
