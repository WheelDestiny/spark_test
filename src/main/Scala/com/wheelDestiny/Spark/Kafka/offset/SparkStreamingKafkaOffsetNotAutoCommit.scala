package com.wheelDestiny.Spark.Kafka.offset

import java.lang

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Durations, StreamingContext}


/**
 * 手动更新偏移量
 */
object SparkStreamingKafkaOffsetNotAutoCommit {
  def main(args: Array[String]): Unit = {
    val group = "wheelDestinyGroup"
    val topic = "wheelDestinyTopic"

    val conf: SparkConf = new SparkConf().setAppName("SparkStreamingKafkaOffsetNotAutoCommit").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Durations.seconds(5))

    //直连kafka的broker地址，sparkStreaming直接连到kafka集群
    val brokers = "s1.hadoop:9092,s3.hadoop:9092,s4.hadoop:9092,s5.hadoop:9092,s6.hadoop:9092,s7.hadoop:9092,s8.hadoop:9092"


    //设置kafka参数
    val kafkaParams = Map(
      "bootstrap.servers" -> brokers,
      "group.id" -> group,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      //修改为手动提交偏移量
      "enable.auto.commit" -> (false: lang.Boolean),
      //earliest  当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
      //latest  当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
      //none  topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常
      "auto.offset.reset" -> "earliest"
    )

    val topics: Array[String] = Array(topic)

    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))

    stream.foreachRDD(rdd =>{
      val ranges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      rdd.foreach{
        line =>{
          println(line.key()+"\t"+line.value())
        }
      }
      for (elem <- ranges) {
        println(s"partition:${elem.partition}\toffset:${elem.untilOffset.toString}")
      }
      //更新偏移量
      stream.asInstanceOf[CanCommitOffsets].commitAsync(ranges)

    })

    ssc.start()
    ssc.awaitTermination()

  }

}
