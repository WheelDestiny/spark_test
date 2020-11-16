package com.wheelDestiny.Spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object SparkSql03_UDAF {
  def main(args: Array[String]): Unit = {
    //sparkConf
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSql01_Demo")
    val sc = new SparkContext(sparkConf)

    //sparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    //进行转换之前需要引入隐式转换规则
    //这里的spark不是包名的含义，是SparkSession对象的名字
    import spark.implicits._

    //创建聚合函数对象
    val udaf: MyAvgUDAF = new MyAvgUDAF
    //注册聚合函数
    spark.udf.register("avgAge",udaf)
    //使用聚合函数
    val frame = spark.read.json("in/user.json")

    //将dateframe转化为表
    frame.createOrReplaceTempView("userTable")

    spark.sql("select avgAge(age) from userTable").show()

    spark.stop()

  }

}
//声明用户自定义的聚合函数
//1，继承UserDefinedAggregateFunction
//2，实现方法
class MyAvgUDAF extends UserDefinedAggregateFunction {
  //输入数据的结构
  override def inputSchema: StructType = {
    //添加结构
    //函数输入的数据结构
    new StructType().add("age",LongType)
  }
  //计算过程中需要的数据结构
  override def bufferSchema: StructType = {
    new StructType().add("sum",LongType).add("count",LongType)
  }
  //数据计算完毕之后的结构类型，即函数最终返回的数据类型
  override def dataType: DataType = {
    DoubleType
  }
  //稳定性，即函数接收到相同的值时结果是否相同
  override def deterministic: Boolean = true
  // 计算之前缓冲器的初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //因为无法通过名称获取到，所以我们只能通过顺序来获取bufferSchema中定义的数据，并为他们初始化
    buffer(0) = 0L
    buffer(1) = 0L
  }
  //根据查询结果更新缓冲区的数据
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //sum
    buffer(0) = buffer.getLong(0)+input.getLong(0)
    //count
    buffer(1) = buffer.getLong(1)+1
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //将多个节点的缓冲区合并在一起
    buffer1(0) = buffer1.getLong(0)+buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1)+buffer2.getLong(1)
  }
  //计算
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble/buffer.getLong(1)
  }
}

