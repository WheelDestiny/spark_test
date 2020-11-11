package com.wheelDestiny.Spark.core

import org.apache.spark.{SparkConf, SparkContext}

object Spark14_Oper13 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    //map算子
    val words = Array("one","two","two","three","three","three")
    val listRDD = sc.makeRDD(words,4)

    val mapRDD = listRDD.map((_, 1))

    println(mapRDD.groupByKey().map(i => (i._1,i._2.size)).collect().mkString(","))
    println(mapRDD.reduceByKey(_ + _).collect().mkString(","))


  }

}
