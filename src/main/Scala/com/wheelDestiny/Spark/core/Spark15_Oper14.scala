package com.wheelDestiny.Spark.core

import org.apache.spark.{SparkConf, SparkContext}

object Spark15_Oper14 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    //map算子
    val mapRDD = sc.makeRDD(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)

//    mapRDD.glom().collect().foreach(i =>{
//      i.foreach(print)
//      println()
//    })
//    (a,3)(a,2)(c,4)
//    (b,3)(c,6)(c,8)
    // 第一个参数为初始值，第二个参数为分区内的计算规则，第三个参数为分区间的计算规则
    val aggRDD = mapRDD.aggregateByKey(0)(List(_,_).max, _ + _)
    // 第一个参数为初始值，第二个参数为分区内分区间的计算规则
    val foldRDD = mapRDD.foldByKey(0)(_+_)

    val combinerRDD = mapRDD.combineByKey(
      (_, 1),
      (i: (Int, Int), j) => (i._1 + j, i._2 + 1) ,
      (i: (Int, Int), j: (Int, Int)) => (i._1 + j._1, i._2 + j._2)
    )
    val sumRDD = mapRDD.combineByKey(i => i, (i: Int, j) => i + j, (i: Int, j: Int) => i + j)

//    sumRDD.collect().foreach(println)

    combinerRDD.map(i => (i._1,i._2._1/i._2._2)).collect().foreach(println)

//    aggRDD.collect().foreach(println)





  }

}
