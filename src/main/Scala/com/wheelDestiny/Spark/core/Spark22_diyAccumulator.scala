package com.wheelDestiny.Spark.core

import java.util

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

object Spark22_diyAccumulator {

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)
    val dataRDD = sc.makeRDD(List(1, 2, 3, 4, 5), 2)
    val dataRDD1 = sc.makeRDD(List("hello","world","hi","scala","height","spark"), 2)


//    var sum:Int = 0
    //使用累加器来共享变量。累加数据

    //创建累加器对象
//    val accumulator = sc.longAccumulator

//    dataRDD.foreach{
//      case i=>{
//        accumulator.add(i)
//      }
//    }
    //获取累加器的值
//    println("sum="+accumulator.value)

    //TODO 创建自定义的累加器对象
    val wordAccumulator = new WordAccumulator
    //TODO 注册累加器
    sc.register(wordAccumulator)

    dataRDD1.foreach{
      case word=>{
        wordAccumulator.add(word)
      }
    }
    println(wordAccumulator)


    sc.stop()
  }
}
//自定义一个累加器
//第一步，继承AccumulatorV2，设定输入输出类型
class WordAccumulator extends AccumulatorV2[String, util.ArrayList[String]] {

  val list = new util.ArrayList[String]()
  //第二步，重写抽象方法
  //判断当前是否是初始化状态
  override def isZero: Boolean = list.isEmpty
  //赋值累加器对象
  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = new WordAccumulator()
  //重置累加器对象
  override def reset(): Unit = list.clear()
  //向累加器中增加数据
  override def add(v: String): Unit = {
    if(v.contains("h")){
      list.add(v)
    }
  }
  //合并，合并两个累加器，主要是合并多个分区的累加器
  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = list.addAll(other.value)
  //获取累加器的结果
  override def value: util.ArrayList[String] = {
    list
  }
}
