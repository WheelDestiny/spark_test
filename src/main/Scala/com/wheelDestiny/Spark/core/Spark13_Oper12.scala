package com.wheelDestiny.Spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

import scala.reflect.ClassTag

object Spark13_Oper12 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    //map算子

    val listRDD = sc.makeRDD(Array((1,"aaa"),(2,"bbb"),(3,"ccc"),(4,"ddd")),4)
//    重分区
//    println(listRDD.partitionBy(new org.apache.spark.HashPartitioner(2)).collect().mkString(","))

//    listRDD.partitionBy(new MyPartitioner(2)).glom().collect().foreach(i =>{
//      i.foreach(print)
//      println()
//    })


//    val value: RDD[(Int, Iterable[(Int, String)])] = listRDD.groupBy(_._1, new HashPartitioner(1))

  }

  class MyPartitioner(partitioner : Int) extends Partitioner{
    override def numPartitions: Int = {
      partitioner
    }

    override def getPartition(key: Any): Int = key match {
      case null => 0
      case _ => nonNegativeMod(key.hashCode, numPartitions)
    }

    def nonNegativeMod(x: Int, mod: Int): Int = {
      val rawMod = x % mod
      rawMod + (if (rawMod < 0) mod else 0)
    }
  }

}
