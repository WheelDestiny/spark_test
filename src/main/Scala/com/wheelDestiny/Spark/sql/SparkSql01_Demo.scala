package com.wheelDestiny.Spark.sql

import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.SparkSession

object SparkSql01_Demo {
  def main(args: Array[String]): Unit = {
    //sparkConf
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSql01_Demo")
    val sc = new SparkContext(sparkConf)

    //sparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()


    val frame = spark.read.json("in/user.json")

    //将dateframe转化为表
    frame.createOrReplaceTempView("userTable")

    //采用sql的语法访问数据
    spark.sql("select * from userTable").show()




//    frame.show()

    spark.stop()

  }

}
