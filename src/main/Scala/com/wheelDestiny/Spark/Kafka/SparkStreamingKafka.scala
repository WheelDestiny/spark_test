package com.wheelDestiny.Spark.Kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, ConsumerStrategy, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Durations, StreamingContext}

import scala.collection.mutable

object SparkStreamingKafka {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkStreamingKafka").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Durations.seconds(5))

    val strings: Array[String] = "wheelDestiny".split(",")

    val kafkaParams: mutable.HashMap[String, Object] = mutable.HashMap[String, Object]()

    kafkaParams += "bootstrap.servers" -> "s1.hadoop:9092,s3.hadoop:9092,s4.hadoop:9092,s5.hadoop:9092,s6.hadoop:9092,s7.hadoop:9092,s8.hadoop:9092"
    kafkaParams += "group.id" -> "wheelDestiny"
    kafkaParams += "key.deserializer" -> classOf[StringDeserializer].getName
    kafkaParams += "value.deserializer" -> classOf[StringDeserializer].getName

    val value: ConsumerStrategy[String, String] = ConsumerStrategies.Subscribe[String, String](strings, kafkaParams)

    val inputDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, value)

    val reduceByKey: DStream[(String, Int)] = inputDStream.flatMap(_.value().split(" ")).map((_, 1)).reduceByKey(_ + _)
    reduceByKey.foreachRDD(r=>{
      println(s"${r.collect().toList}")
    })

    ssc.start()
    ssc.awaitTermination()



  }

}
