package com.wheelDestiny.Spark.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.deploy.

object Spark16_Oper15 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    //map算子
    val line = sc.textFile("in/word.txt")

    val value1 = line.map(i => {
      val files = i.split(" ")
      ((files(1), files(4)), 1)
    })
    val value2 = value1.reduceByKey(_ + _)
    val value3 = value2.map(i => {
      (i._1._1, (i._1._2, i._2))
    })
    val value4 = value3.groupByKey()
    val value5 = value4.map(i => {
      (i._1, i._2.toList.sortWith((i, j) => (i._2 > j._2)).take(3))
    })

    value5.collect().foreach(println)






  }

}
