package com.wheelDestiny.Spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//创建spark上下文对象的类即为spark的Driver对象
object Spark01_RDD {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(conf)
    //创建RDD
    //1)从内存中创建makeRDD
    //默认分区数量为本地cpu核数，不能小于2
    //自己设置分区数量可以在后面加一个参数
    val intRDD = sc.makeRDD(List(1, 2, 3, 4,5),2)
    //2)从内存中创建parallelize
    val arrayRDD = sc.parallelize(Array(1, 2, 3, 4))
    //3)从外部存储中创建
    //默认情况下读取的是项目路径，也可以读取其他路径，比如HFDS
    //默认从文件中读取的数据都是字符串类型，默认分区数为cpu核数和2比较的较小值
    //实际上读取文件时，传递的分区数为最小分区数，但是最小分区数并不一定就是最终的分区数，这取决于Hadoop的分片规则
    val value: RDD[String] = sc.textFile("in" , 2)

//    arrayRDD.collect().foreach(println)

    value.saveAsTextFile("output")

  }

}
