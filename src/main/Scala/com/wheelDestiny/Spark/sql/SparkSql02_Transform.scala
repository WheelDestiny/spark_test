package com.wheelDestiny.Spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object SparkSql02_Transform {
  def main(args: Array[String]): Unit = {
    //sparkConf
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSql01_Demo")
    val sc = new SparkContext(sparkConf)

    //sparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    //进行转换之前需要引入隐式转换规则
    //这里的spark不是包名的含义，是SparkSession对象的名字
    import spark.implicits._


    //创建RDD
    val rdd = spark.sparkContext.makeRDD(List((1, "zhao", 10), (2, "qian", 20), (3, "sun", 30)))

    //转换为DF
    val df = rdd.toDF("id", "name", "age")

    //转换为DS
    val ds: Dataset[User] = df.as[User]
    //转换为DF
    val df1 = ds.toDF()
    //转换为RDD
    val rdd1: RDD[Row] = df1.rdd

    rdd1.foreach(row=>{
      //获取数据时，可以通过索引访问数据
      println(row.getInt(0))
    })

    //RDD->DateSet
    val userRDD: RDD[User] = rdd.map { case (id, name, age) => {
      User(id, name, age)
    }
    }
    val userDS: Dataset[User] = userRDD.toDS()

    val rdd2: RDD[User] = userDS.rdd

    rdd2.foreach(println)


    spark.stop()

  }

}

case class User(id:Int,name:String,age:Int)

