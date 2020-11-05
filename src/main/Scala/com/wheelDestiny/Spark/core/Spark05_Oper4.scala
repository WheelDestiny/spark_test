package com.wheelDestiny.Spark.core

import org.apache.spark.{SparkConf, SparkContext}

object Spark05_Oper4 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    //map算子
    val listRDD = sc.makeRDD(1 to 21 ,4)

    //将一个分区的数据放入一个数组中
    val glomRDD = listRDD.glom()

    glomRDD.collect().foreach(i =>{
      println(i.mkString(","))
    })




  }

}
