package com.wheelDestiny.Spark.hainiu

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDLearn {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val value= sc.makeRDD(Array(1, 2, 3, 4),4)
    val value1: RDD[(Int, String)] = value.map((_, "123"))
    value1.aggregateByKey("11")(_+_,_+"1111"+_)


    value1.collect()



//    value.sortBy(_)

    println(value.getNumPartitions)
  }


}
