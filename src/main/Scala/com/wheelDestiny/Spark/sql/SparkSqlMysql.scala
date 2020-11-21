package com.wheelDestiny.Spark.sql

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkSqlMysql {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HainiuSqlData")
//    conf.set("spark.shuffle.partitions", "1")
    val sc = new SparkContext(conf)


    val sqlc = new SQLContext(sc)
    val jdbc: DataFrame = sqlc.jdbc("jdbc:mysql://nn2.hadoop:3306/hainiucralwer?user=hainiu&password=12345678", "hainiu_web_seed")
    //读取jdbc时使用了一个和partition
    println(jdbc.rdd.getNumPartitions)

    val jdbc1: DataFrame = sqlc.jdbc("jdbc:mysql://nn2.hadoop:3306/hainiucralwer?user=hainiu&password=12345678","hainiu_web_seed_internally")
    jdbc.createOrReplaceTempView("hainiu_web_seed")
    jdbc1.createOrReplaceTempView("hainiu_web_seed_internally")

    val join: DataFrame = sqlc.sql(
      """
        |select * from
        |hainiu_web_seed t1
        |inner join hainiu_web_seed_internally t2
        |on t1.md5 = t2.md5 limit 20
        |""".stripMargin)
    join.show()



  }

}
