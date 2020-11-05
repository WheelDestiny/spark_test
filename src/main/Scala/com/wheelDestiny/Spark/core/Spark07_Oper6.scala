package com.wheelDestiny.Spark.core

import org.apache.spark.{SparkConf, SparkContext}

object Spark07_Oper6 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    //map算子
    val listRDD = sc.makeRDD(1 to 20)

    val filterRDD = listRDD.filter(_ % 2 == 0)

    filterRDD.collect().foreach(println)




  }

}
