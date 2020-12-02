package com.wheelDestiny.Spark.Kafka.offset

import java.lang

import kafka.api.PartitionOffsetRequestInfo
import kafka.common.TopicAndPartition
import kafka.javaapi.consumer.SimpleConsumer
import kafka.javaapi.{OffsetRequest, OffsetResponse, PartitionMetadata, TopicMetadataRequest, TopicMetadataResponse}
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Durations, StreamingContext}

import scala.collection.mutable


/**
 * 偏移量保存到zk中
 * 处理数据时使用spark-core的方式
 * 解决由于上一次记录的offset已经在kafka中过期而导致的程序无法启动问题
 *
 */
object SparkStreamingKafkaOffsetZKRecovery {

  def main(args: Array[String]): Unit = {
    //指定组名
    val group = "wheelDestinyGroup"
    val conf: SparkConf = new SparkConf().setAppName("SparkStreamingKafkaOffsetZKTransform").setMaster("local[*]")

    val ssc = new StreamingContext(conf, Durations.seconds(5))

    val topic1 = "wheelDestinyTopic"
//    val topic2 = "wheelDestiny"
    val brokerList = "s1.hadoop:9092,s3.hadoop:9092,s4.hadoop:9092,s5.hadoop:9092,s6.hadoop:9092,s7.hadoop:9092,s8.hadoop:9092"

    val zkNodes = "nn1.hadoop:2181,nn2.hadoop:2181,s1.hadoop:2181"
    //创建流时要消费的topic集合，可以同时消费多个topic
    val topics = Set(topic1)

    //用于在zk中保存偏移量的zk路径
    val topicDirs = new ZKGroupTopicDirs(group, topic1)
    //得到zk中保存偏移量的路径的string
    val zkTopicPath = s"${topicDirs.consumerOffsetDir}"

    val kafkaParams = Map(
      "bootstrap.servers" -> brokerList,
      "group.id" -> group,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "enable.auto.commit" -> (false: lang.Boolean),
      //earliest  当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
      //latest  当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
      //none  topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常
      "auto.offset.reset" -> "latest"
    )

    //创建一个空的kafkaStream。根据是否有历史偏移量创建实例
    var kafkaStream:InputDStream[ConsumerRecord[String,String]] = null

    //如果存在历史偏移量，用一个HashMap来存放每个TopicPartition对应的offset
    val historyOffset = new mutable.HashMap[TopicPartition, Long]()

    //创建zk客户端，可以从zk中读取偏移量数据，并更新偏移量
    val zkClient = new ZkClient(zkNodes)

    //根据我们生成的zk路径查询这个路径下是否有分区，有分区即说明有历史数据，如果没有分区，说明没有历史数据
    val partitionNum: Int = zkClient.countChildren(zkTopicPath)

    //判断是否有历史offset
    if(partitionNum > 0){
      for(i <- 0 until partitionNum){
        val pOffset: String = zkClient.readData[String](s"${zkTopicPath}/${i}")
        val topicPartition = new TopicPartition(topic1, i)
        //将所有的分区对应的offset放到historyOffset
        historyOffset+=topicPartition->pOffset.toLong
      }

      //处理因为长时间没有启动程序后再次启动时上次的偏移量已经在kafka中过期了的问题
      import scala.collection.mutable.Map
      //存储kafka中每个partition当前可用的最小偏移量
      val earliestOffsets: Map[Long, Long] = Map[Long, Long]()
      val consumer: SimpleConsumer = new SimpleConsumer("s1.hadoop", 9092, 100000, 64 * 1024, "leaderLookup" + System.currentTimeMillis())

      //这一部分需要一些java的api，我们引一下java和scala的转换
      import scala.collection.convert.wrapAll._
      val request: TopicMetadataRequest = new TopicMetadataRequest(topics.toList)
      val response: TopicMetadataResponse = consumer.send(request)
      consumer.close()

      val metadatas: mutable.Buffer[PartitionMetadata] = response.topicsMetadata.flatMap(f => f.partitionsMetadata)
      //从kafka集群中得到当前每个partition最早的offset值
      metadatas.map(f=>{
        val partitionId: Int = f.partitionId
        val leaderHost: String = f.leader.host
        val leaderPort: Int = f.leader.port
        val clientName: String = "Client_" + topic1 + "_" + partitionId
        val consumer = new SimpleConsumer(leaderHost, leaderPort, 100000, 64 * 1024, clientName)

        val topicAndPartition = new TopicAndPartition(topic1, partitionId)
        val requestInfo = new mutable.HashMap[TopicAndPartition, PartitionOffsetRequestInfo]()
        requestInfo.put(topicAndPartition,new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.EarliestTime,1))

        val request = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion, clientName)
        val response: OffsetResponse = consumer.getOffsetsBefore(request)
        val offsets: Array[Long] = response.offsets(topic1, partitionId)
        consumer.close()

        earliestOffsets += ((partitionId, offsets(0)))
      })

      //和历史offset做对比
//      val nowOffsets: mutable.HashMap[TopicPartition, Long] = historyOffset.map(offset => {
//        val earliestOffset: Long = earliestOffsets(offset._1.partition())
//        if (offset._2 >= earliestOffset) {
//          offset
//        } else {
//          (offset._1, earliestOffset)
//        }
//      })
      val nowOffsets: mutable.Map[TopicPartition, Long] = earliestOffsets.map(offset => {
        val hisOffset: Long = historyOffset.getOrElse(new TopicPartition(topic1, offset._1.toInt), 0L)
        if (offset._2 >= hisOffset) {
          (new TopicPartition(topic1, offset._1.toInt), offset._2)
        } else {
          (new TopicPartition(topic1, offset._1.toInt), hisOffset)
        }
      })




      kafkaStream = KafkaUtils.createDirectStream[String,String](ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String,String](topics,kafkaParams,nowOffsets))
    }else{
      kafkaStream = KafkaUtils.createDirectStream[String,String](ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String,String](topics,kafkaParams))
    }

    //通过rdd转换得到偏移量的范围

    var offsetRanges: Array[OffsetRange] = Array[OffsetRange]()

    kafkaStream.foreachRDD(kafkaRDD => {
      if(!kafkaRDD.isEmpty()){
        offsetRanges = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges
        val dataRDD: RDD[String] = kafkaRDD.map(_.value())

        dataRDD.foreachPartition(
          partition =>{
            partition.foreach(
              println
            )
          }
        )
        for(o <- offsetRanges){
          val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
          println(s"${zkPath}__${o.untilOffset.toString}")
          ZkUtils(zkClient,false).updatePersistentPath(zkPath,o.untilOffset.toString)
        }
      }
    })
    ssc.start()
    ssc.awaitTermination()

  }


}
