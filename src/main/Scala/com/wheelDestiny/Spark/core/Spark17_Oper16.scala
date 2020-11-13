package com.wheelDestiny.Spark.core

import org.apache.spark.{SparkConf, SparkContext}

object Spark17_Oper16 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(conf)
    //使用检查点一定要设定检查点保存的目录
    sc.setCheckpointDir("cp")

    val rdd = sc.makeRDD(List(1,1,2,3,4,5,5,5))
    val mapRDD = rdd.map((_, 1))

    val reduceRDD = mapRDD.reduceByKey(_ + _)
    reduceRDD.checkpoint()
    reduceRDD.foreach(println)
    println(reduceRDD.toDebugString)



//    println(strRDD.collect().mkString(","))

  }

}
