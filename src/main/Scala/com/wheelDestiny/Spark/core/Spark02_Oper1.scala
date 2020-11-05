package com.wheelDestiny.Spark.core

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Oper1 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    //map算子
    val listRDD = sc.makeRDD(1 to 10)

    //所有RDD算子的计算功能全部由Executor执行，
    // 所以发送给Executor的算子中所需要的对象都必须是可序列化的，如果是不可序列化的对象会报错
//    val mapRDD = listRDD.map(_ * 2)
//
//    mapRDD.collect().foreach(println)



    //mapPartitions可以对RDD中所有的分区进行遍历，具体每个分区中的数据需要做的操作，由开发人员自行实现
    //对比map算子，mapPartitions算子向Executor发送算子的次数要少更多，相比较就大幅度减少了网络传输带来的压力，提高了计算效率
    //可能会出现内存溢出，当向一个Executor发送的数据大于当前机器的内存，就可能会导致内存溢出,
    // 即使分次发送数据整个spark在整个任务计算结束之前都不会释放内存，所以使用时要具体情况具体考虑
    val mapRDD = listRDD.mapPartitions(i => {
        i.map(_ * 2)
    })
    mapRDD.collect().foreach(println)



  }

}
