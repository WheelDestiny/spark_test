package com.wheelDestiny.Spark.core

import org.apache.spark.{SparkConf, SparkContext}

object Spark11_Oper10 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    //map算子
    val listRDD = sc.makeRDD(Range(1,64),4)
    listRDD.collect().foreach(print)
    println(listRDD.partitions.size)

    //repartition实际上就是使用coalesce，第二个参数为true，开启shuffle
    val repartitionRDD = listRDD.repartition(2)
    repartitionRDD.collect().foreach(print)


    println(repartitionRDD.partitions.size)




  }

}
