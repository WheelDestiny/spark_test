package com.wheelDestiny.Spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}
import org.apache.spark.sql.{Dataset, Encoder, Encoders, Row, SparkSession, TypedColumn}
import org.apache.spark.{SparkConf, SparkContext}

object SparkSql04_UDAF_Class {
  def main(args: Array[String]): Unit = {
    //sparkConf
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSql01_Demo")
    val sc = new SparkContext(sparkConf)

    //sparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    //进行转换之前需要引入隐式转换规则
    //这里的spark不是包名的含义，是SparkSession对象的名字
    import spark.implicits._

    val udaf = new MyAvgClassUDAF
    val frame = spark.read.json("in/user.json")

    //将聚合函数转换为查询列
    //因为在这里的UDAF函数需要的是一个对象而不是某个字段，所以我们需要把这个函数转化为一个查询列，
    // 根据DS的数据结构
    val avgCol: TypedColumn[UserBean, Double] = udaf.toColumn.name("avgAge")
    //  我们就需要根据设定的UDAF的泛型，用DS对类型做约束
    val userDS: Dataset[UserBean] = frame.as[UserBean]

    userDS.select(avgCol).show()


    spark.stop()

  }

}
//定义必要的样例类
case class UserBean(name:String,age:BigInt)
case class AvgBuffer(var sum:BigInt,var count:Int)

//声明用户自定义的聚合函数(强类型)
//1，继承Aggregator，并设定泛型
//2，实现方法
class MyAvgClassUDAF extends Aggregator[UserBean,AvgBuffer,Double]{
  //初始化缓冲区
  override def zero: AvgBuffer = {
    AvgBuffer(0,0)
  }
  //将每次传入的数据聚合
  override def reduce(b: AvgBuffer, a: UserBean): AvgBuffer = {
    b.sum +=a.age
    b.count+=1
    b
  }
  //分区间的数据合并
  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    b1.sum+=b2.sum
    b1.count+=b2.count
    b1
  }
  //最终的计算结果
  override def finish(reduction: AvgBuffer): Double = {
    reduction.sum.toDouble/reduction.count
  }
  //缓冲区转码
  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product
  //最终输出转码
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}