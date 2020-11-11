package com.wheelDestiny.Spark.core

import org.apache.spark.{SparkConf, SparkContext}

object Spark12_Oper11 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    //map算子
    val listRDD1 = sc.makeRDD(Range(1,8))
    val listRDD2 = sc.makeRDD(Range(2,9))

//    val unionRDD = listRDD1.union(listRDD2)
//    val subtractRDD = listRDD1.subtract(listRDD2)

    val intersectionRDD = listRDD1.intersection(listRDD2)

//    println(unionRDD.collect().mkString(","))
//    println(subtractRDD.collect().mkString(","))
    println(intersectionRDD.collect().mkString(","))






  }

}
