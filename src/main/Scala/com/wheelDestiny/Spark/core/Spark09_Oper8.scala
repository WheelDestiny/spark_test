package com.wheelDestiny.Spark.core

import org.apache.spark.{SparkConf, SparkContext}

object Spark09_Oper8 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    //map算子
    val listRDD = sc.makeRDD(List(1,2,2,3))

    //使用distinct算子对数据去重，可以设置默认分区数量，减少分区浪费
    //全局distinct会导致shuffle，而shuffle的过程是很慢的
    val distinctRDD = listRDD.distinct(1)

    distinctRDD.collect().foreach(println)
    distinctRDD.saveAsTextFile("outPut")




  }

}
