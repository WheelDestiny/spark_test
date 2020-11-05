package com.wheelDestiny.Spark.core

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Oper2 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    //map算子
    val listRDD = sc.makeRDD(1 to 10,2)

    val mapRDD = listRDD.mapPartitionsWithIndex {
      case (num, datas) => {
        datas.map((num,_))
      }
    }
    val mapRDD1 = listRDD.mapPartitionsWithIndex ((num,datas) => datas.map((num,_)) )


    mapRDD1.collect().foreach(println)



  }

}
