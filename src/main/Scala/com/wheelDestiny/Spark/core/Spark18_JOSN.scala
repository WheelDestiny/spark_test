package com.wheelDestiny.Spark.core

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

object Spark18_JOSN {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(conf)
    val json = sc.textFile("in/user.json")
    val result = json.map(JSON.parseFull)
    result.foreach(println)






  }

}
