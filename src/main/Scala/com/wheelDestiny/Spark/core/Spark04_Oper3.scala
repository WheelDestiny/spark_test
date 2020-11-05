package com.wheelDestiny.Spark.core

import org.apache.spark.{SparkConf, SparkContext}

object Spark04_Oper3 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    //map算子
    val listRDD = sc.makeRDD(Array(List(1,2),List(3,4)))

    //flatMap
    val mapRDD = listRDD.flatMap(_.map(_*2))


    mapRDD.collect().foreach(println)



  }

}
