package com.wheelDestiny.Spark.core

import org.apache.spark.{SparkConf, SparkContext}

object Spark21_shareData {

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)
    val dataRDD = sc.makeRDD(List(1, 2, 3, 4, 5), 2)


//    var sum:Int = 0
    //使用累加器来共享变量。累加数据

    //创建累加器对象
    val accumulator = sc.longAccumulator

    dataRDD.foreach{
      case i=>{
        accumulator.add(i)
      }
    }
    //获取累加器的值
    println("sum="+accumulator.value)

    sc.stop()



  }

}
