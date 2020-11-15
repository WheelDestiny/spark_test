package com.wheelDestiny.Spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount{
  def main(args: Array[String]): Unit = {
    //local模式
    //创建SparkConf对象
    //设定Spark计算框架的运行环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(conf)

    //读取文件，将文件内容按行读取
    val lines: RDD[String] = sc.textFile("in")

    val words = lines.flatMap(_.split(" "))

    val kv = words.map((_, 1))

    val res = kv.reduceByKey(_ + _)
    val tuples = res.collect()
    tuples.foreach(println)

//    println(sc)




  }
}
