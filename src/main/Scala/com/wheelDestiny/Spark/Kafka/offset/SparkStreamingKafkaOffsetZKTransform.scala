package com.wheelDestiny.Spark.Kafka.offset

import java.lang

import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, ConsumerStrategy, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Durations, StreamingContext}

import scala.collection.mutable

/**
 * 保存偏移量到ZK中，
 */
object SparkStreamingKafkaOffsetZKTransform {
  def main(args: Array[String]): Unit = {

    val group = "wheelDestinyGroup"
    val conf: SparkConf = new SparkConf().setAppName("SparkStreamingKafkaOffsetZKTransform").setMaster("local[*]")

    val ssc: StreamingContext = new StreamingContext(conf, Durations.seconds(5))

    val topic = "wheelDestinyTopic"
    //直连kafka的broker地址，sparkStreaming直接连到kafka集群
    val brokers = "s1.hadoop:9092,s3.hadoop:9092,s4.hadoop:9092,s5.hadoop:9092,s6.hadoop:9092,s7.hadoop:9092,s8.hadoop:9092"
    //指定zk地址，更新消费的偏移量时使用
    val zkQuorum = "nn1.hadoop:2181,nn2.hadoop:2181,s1.hadoop:2181"
    //topic集合，可以同时消费多个topic
    val topics = Set(topic)
    //在ZK中保存偏移量的路径
    val dirs: ZKGroupTopicDirs = new ZKGroupTopicDirs(group, topic)
    //具体的路径字符串，格式是"/consumers/${group}/offsets/${topic}"
    val zkTopicPath = s"${dirs.consumerOffsetDir}"

    //设置kafka参数
    val kafkaParams = Map(
      "bootstrap.servers" -> brokers,
      "group.id" -> group,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "enable.auto.commit" -> (false: lang.Boolean),
      //earliest  当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
      //latest  当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
      //none  topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常
      "auto.offset.reset" -> "earliest"
    )

    //定义一个空的kafkaStream，之后可以根据是否有历史偏移量进行选择
    var kafkaStream:InputDStream[ConsumerRecord[String,String]] = null

    //存放zk中读取的历史offset
    val hisOffsets: mutable.HashMap[TopicPartition, Long] = new mutable.HashMap[TopicPartition, Long]

    //创建zk客户端，读取历史偏移量
    val zkClient = new ZkClient(zkQuorum)

    //从zk中查询该数据路径下是否有offset
    val child: Int = zkClient.countChildren(zkTopicPath)

    if(child>0){
      for(i <- 0 until child){
        //获取每个分区的offset
        val partitionOffset: String = zkClient.readData[String](s"$zkTopicPath/$i")
        //将获取到的偏移量放入集合
        hisOffsets += new TopicPartition(topic,i)->partitionOffset.toLong
      }

      val subScribe: ConsumerStrategy[String, String] = ConsumerStrategies.Subscribe[String, String](topics, kafkaParams, hisOffsets)
      //通过从zk中获取到的偏移量来创建直连kafka的DStream
      kafkaStream = KafkaUtils.createDirectStream[String,String](ssc,LocationStrategies.PreferConsistent,subScribe)
    }else{
      //没有在zk中读取到历史offset
      kafkaStream = KafkaUtils.createDirectStream[String,String](ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))
    }

    //通过rdd转换得到偏移量的范围,即本次代码消费到了什么位置，用来在消费后更新zk中的offset
    var offsetRanges: Array[OffsetRange] = Array[OffsetRange]()

    //从kafka读取的消息，DStream的Transform方法可以将当前批次的RDD获取出来
    //然后将RDD的偏移量取出来。然后将RDD返回到DStream
    val transform: DStream[ConsumerRecord[String, String]] = kafkaStream.transform(rdd => {
      //得到该RDD对应kafka消息的offset，该RDD是一个KafkaRDD，可以获取偏移量范围
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })
    val map: DStream[String] = transform.map(_.value())

    map.foreachRDD(rdd =>{
      //执行具体的消费逻辑
      rdd.foreachPartition(p =>{
        p.foreach(word => {
          println(word)
        })
      }
      )

      //更新zk中的offset
      for (o <- offsetRanges){
        val zkPath = s"${dirs.consumerOffsetDir}/${o.partition}"
        println(s"${zkPath}__${o.untilOffset.toString}")
        ZkUtils(zkClient,false).updatePersistentPath(zkPath,o.untilOffset.toString)
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
