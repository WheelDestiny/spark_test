package com.wheelDestiny.Spark.core

import org.apache.spark.{SparkConf, SparkContext}

object Spark10_Oper9 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    //map算子
    val listRDD = sc.makeRDD(List(1,2,2,3,40,5,5,5,4,3,1,1,2,3,1,0))
    println(listRDD.partitions.size)

    val value = listRDD.sortBy((i => i), false, 2)
    value.collect().foreach(println)


    //使用coalesce可以缩减分区数量
    //可以通过第二个参数设置是否发生shuffle
    val coalesceRDD = listRDD.coalesce(2)

//    println(coalesceRDD.partitions.size)




  }

}
