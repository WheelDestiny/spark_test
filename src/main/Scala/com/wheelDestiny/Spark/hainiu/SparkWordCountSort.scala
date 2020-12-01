package com.wheelDestiny.Spark.hainiu

import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCountSort {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    val outPath = "in"



  }

}
