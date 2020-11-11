package com.wheelDestiny.Spark.core

import org.apache.spark.{SparkConf, SparkContext}

object Spark08_Oper7 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    //map算子
    val listRDD = sc.makeRDD(1 to 20)

    //从制定的数据集合中进行抽样处理，根据不同的算法进行抽样,
    // 第一个参数为抽样算法，第二个参数是打分标准,第三个参数是计算分数的根源
    //true表示有放回的抽样，false表示无放回的抽样
    val sampleRDD = listRDD.sample(false,1,4)

    sampleRDD.collect().foreach(println)




  }

}
