package com.wheelDestiny.Spark.core

import org.apache.spark.{SparkConf, SparkContext}

object Spark22_broadcast {

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)
    val rdd = sc.makeRDD(List((1, "a"), (2, "b"), (3, "c")))

    val list = List((1,1),(2,2),(3,3))
    //构建广播变量
    val broadcast = sc.broadcast(list)

    val resultRDD = rdd.map {
      case (key, value) => {
        var v2:Any = null
        for (t <- broadcast.value) {
          if (key == t._1) {
            v2 = t._2
          }
        }
        (key, (value, v2))
      }
    }
    resultRDD.foreach(println)






    sc.stop()
  }
}
