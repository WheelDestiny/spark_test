package com.wheelDestiny.Spark.Kafka

import java.io.{BufferedReader, InputStreamReader}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileStatus, FileSystem, Path}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, ConsumerStrategy, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable
import scala.collection.mutable.Map
import scala.collection.mutable.ListBuffer

/**
 * 使用广播变量实现热加载
 */
object SparkStreamingKafkaBroadCastUpdate {

  def main(args: Array[String]): Unit = {
    val topic = "wheelDestiny"
    val brokers = "s1.hadoop:9092,s3.hadoop:9092,s4.hadoop:9092,s5.hadoop:9092,s6.hadoop:9092,s7.hadoop:9092,s8.hadoop:9092"


    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamingKafkaBroadCastUpdate")
    conf.set("spark.serializer", classOf[KryoSerializer].getName)
    val streamingContext = new StreamingContext(conf, Durations.seconds(5))

    val kafkaParams = new mutable.HashMap[String, Object]()
    kafkaParams += "bootstrap.servers" -> brokers
    kafkaParams += "group.id" -> "wheelDestiny"

    //这两个的deserializer是sparkStreaming作为consumer使用的
    kafkaParams += "key.deserializer" -> classOf[StringDeserializer].getName
    kafkaParams += "value.deserializer" -> classOf[StringDeserializer].getName

    //这里包含了3个DStream
    val directDStreamList = new ListBuffer[InputDStream[ConsumerRecord[String, String]]]

    for (i <- 0 until 3) {
      val partitions = new ListBuffer[TopicPartition]
      for (j <- 6 * i until 6 * (i + 1)) {
        val partition = new TopicPartition(topic, j)
        partitions += partition
      }
      val value: ConsumerStrategy[String, String] = ConsumerStrategies.Assign(partitions, kafkaParams)
      val directDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(streamingContext, LocationStrategies.PreferConsistent, value)
      directDStreamList += directDStream
    }

    val unionDStream: DStream[ConsumerRecord[String, String]] = streamingContext.union(directDStreamList)

    val words: DStream[String] = unionDStream.flatMap(_.value().split(" "))

    var mapBroadCast: Broadcast[Map[String, String]] = streamingContext.sparkContext.broadcast(Map[String, String]())

    val updateInterval = 10000L
    var lastUpdateTime = 0L

    val matchAcc: LongAccumulator = streamingContext.sparkContext.longAccumulator
    val noMatchAcc: LongAccumulator = streamingContext.sparkContext.longAccumulator
    words.foreachRDD(r => {
      if (mapBroadCast.value.isEmpty || System.currentTimeMillis() - lastUpdateTime >= updateInterval) {
        val map: mutable.Map[String, String] = Map[String, String]()
        val path = "D:\\dictionary"
        val fs: FileSystem = FileSystem.get(new Configuration())
        val fileStatus: Array[FileStatus] = fs.listStatus(new Path(path))
        for (f <- fileStatus) {
          val filePaths: Path = f.getPath
          val stream: FSDataInputStream = fs.open(filePaths)
          val reader = new BufferedReader(new InputStreamReader(stream))
          var line: String = reader.readLine()
          while (line != null) {
            val strings: Array[String] = line.split("\t")
            map += strings(0) -> strings(1)
            line = reader.readLine()
          }
        }
        mapBroadCast.unpersist()
        mapBroadCast = streamingContext.sparkContext.broadcast(map)
        lastUpdateTime = System.currentTimeMillis()
      }
      println(s"broadCast:${mapBroadCast.value}")

      if (!r.isEmpty()) {
        r.foreachPartition(
          it => {
            val cast: mutable.Map[String, String] = mapBroadCast.value

            import scala.util.control.Breaks._

            for (f <- it) {
              breakable {
                if(f == null){
                  break()
                }
                val countryCode: String = f
                val countryName: String = cast.getOrElse(countryCode, null)
                if (countryName == null) {
                  noMatchAcc.add(1L)
                } else {
                  matchAcc.add(1L)
                }
                println(s"${countryCode}\t${countryName}")
              }
            }

            //            record.foreach(r =>{
            //              val countryCode: String = r.value()
            //              val countryName: String = cast.getOrElse(countryCode, null)
            //              if(countryName == null){
            //                noMatchAcc.add(1L)
            //              }else{
            //                matchAcc.add(1L)
            //              }
            //              println(s"${countryCode}\t${countryName}")
            //            })
          }
        )
        println(s"match:${matchAcc.count},noMatch:${noMatchAcc.count}")

        //累加器清零
        matchAcc.reset()
        noMatchAcc.reset()
      }
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
