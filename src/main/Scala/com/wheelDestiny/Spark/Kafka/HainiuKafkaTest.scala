package com.wheelDestiny.Spark.Kafka

import java.util
import java.util.Properties

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.wheelDestiny.Spark.hainiu.Hbase
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, Partitioner, ProducerRecord}
import org.apache.kafka.common.{Cluster, TopicPartition}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

class MyPartitioner extends Partitioner {
  override def partition(topic: String, key: Any, keyBytes: Array[Byte], value: Any, valueBytes: Array[Byte], cluster: Cluster): Int = {
    0
  }

  override def close(): Unit = {

  }

  override def configure(configs: util.Map[String, _]): Unit = {

  }
}


class HainiuProducer extends Actor {
  private var producer: KafkaProducer[String, KafkaSerializeTest] = _

  override def preStart(): Unit = {
    val pro = new Properties()
    pro.setProperty("bootstrap.servers",
      "s1.hadoop:9092,s3.hadoop:9092,s4.hadoop:9092,s5.hadoop:9092,s6.hadoop:9092,s7.hadoop:9092,s8.hadoop:9092")
    pro.setProperty("key.serializer", classOf[StringSerializer].getName)
    pro.setProperty("value.serializer", classOf[HainiuKafkaSerializer].getName)
    pro.setProperty("partitioner.class", "com.wheelDestiny.Spark.hainiu.Hbase.MyPartitioner")
    producer = new KafkaProducer[String, KafkaSerializeTest](pro)
  }

  override def receive: Receive = {
    case topic: String => {
      var num = 1
      while (true) {
        println(s"producer:${topic},${num}")
        this.producer.send(new ProducerRecord[String, KafkaSerializeTest](topic, KafkaSerializeTest(num)))
        if (num > 10) num = 0
        num += 1
        Thread.sleep(2000)
      }
    }
  }
}

object HainiuProducer {}

case class KafkaSerializeTest(var value: Int)

class HainiuConsumer extends Actor {
  private var consumer: KafkaConsumer[String, String] = _

  override def preStart(): Unit = {
    val pro = new Properties()
    //    pro.setProperty("zookeeper.connect", "nn1.hadoop:2181,nn2.hadoop:2181,s1.hadoop:2181")
    pro.setProperty("bootstrap.servers",
      "s1.hadoop:9092,s2.hadoop:9092,s3.hadoop:9092,s4.hadoop:9092,s5.hadoop:9092,s6.hadoop:9092,s7.hadoop:9092,s8.hadoop:9092")
    pro.setProperty("key.deserializer", classOf[StringDeserializer].getName)
    pro.setProperty("value.deserializer", classOf[HainiuKafkaDeserializer].getName)
    pro.setProperty("group.id", "wheelDestiny")
    pro.setProperty("enable.auto.commit", "true")
    pro.setProperty("auto.offset.reset", "earliest")
    pro.setProperty("auto.commit.interval.ms", "1000")

    consumer = new KafkaConsumer[String, String](pro)
  }

  override def receive: Receive = {
    case topic: String => {
      //订阅模式，轮询消费
      //      consumer.subscribe(java.util.Arrays.asList(topic))
      //分配模式，消费指定分区
      consumer.assign(java.util.Arrays.asList(new TopicPartition(topic, 0)))
      while (true) {
        val record: ConsumerRecords[String, String] = consumer.poll(100)

        import scala.collection.convert.wrapAll._
        for (r <- record) {
          println(s"consumer--topic:${topic},offset:${r.offset()},key:${r.key()},value:${r.value()}")
        }
      }
    }
  }
}

object HainiuConsumer {}

object HainiuKafkaTest {
  def main(args: Array[String]): Unit = {
    val driver = ActorSystem("HainiuKafkaTest")
    val producer: ActorRef = driver.actorOf(Props[Hbase.HainiuProducer](new Hbase.HainiuProducer()), "producer")
    val consumer: ActorRef = driver.actorOf(Props[Hbase.HainiuConsumer](new Hbase.HainiuConsumer()), "consumer")


    val topic = "wheelDestiny"
    producer ! topic
    consumer ! topic


  }

}
