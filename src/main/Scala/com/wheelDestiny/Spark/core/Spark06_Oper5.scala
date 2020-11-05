package com.wheelDestiny.Spark.core

import org.apache.spark.{SparkConf, SparkContext}

object Spark06_Oper5 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    //map算子
    val listRDD = sc.makeRDD(1 to 20)

    //将一个分区的数据放入一个数组中
    //分组后的数据形成了两个元素的元组，第一个元素是key，第二个元素是相同key的元素集合
    val glomRDD = listRDD.groupBy(_%2)

    glomRDD.collect().foreach(list =>{
      print(list._1+" : ")
      println(list._2.mkString(","))
    } )




  }

}
