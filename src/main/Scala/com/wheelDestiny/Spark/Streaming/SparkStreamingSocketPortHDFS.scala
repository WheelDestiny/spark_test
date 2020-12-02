package com.wheelDestiny.Spark.Streaming

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.{SparkConf}

object SparkStreamingSocketPortHDFS {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamingSocketPortHDFS")
    val streamingContext = new StreamingContext(conf,Durations.seconds(5))

    val socketDS: ReceiverInputDStream[String] = streamingContext.socketTextStream("op.hadoop", 7654)
    val reduceByKey: DStream[(String, Int)] = socketDS.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    //使用foreachRDD触发行动
    reduceByKey.foreachRDD(r=>{
      if(!r.isEmpty()){
        val value: RDD[Nothing] = r.mapPartitionsWithIndex((partitionId, f) => {
          val configuration = new Configuration()
          val fs: FileSystem = FileSystem.get(configuration)
          val list: List[(String, Int)] = f.toList

          if (list.length > 0) {
            val format: String = new SimpleDateFormat("yyyyMMddHH").format(new Date)
            val path = new Path(s"hdfs://ns1/user/wheeldestiny26/spark/SparkStreamingSocketPortHDFS/${format}_${partitionId}")

            //这里要判断一下要写入的文件是否存在，如果存在则进行追加，如果不存在则执行创建
            //append只再HDFS集群上支持，本地文件系统不支持append
            val stream: FSDataOutputStream = if (fs.exists(path)) {
              println(path+"append")
              fs.append(path)
            } else {
              println(path+"create")
              fs.create(path)
            }
            list.foreach(
              line => {
                stream.write(s"${line._1}\t${line._2}\n".getBytes("UTF-8"))
              }
            )
            stream.close()
          }
          Nil.toIterator
        })
        value.foreach(f=>f)
      }
    })

    streamingContext.start()
    streamingContext.awaitTermination()


  }

}
