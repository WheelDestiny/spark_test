package com.wheelDestiny.Spark.sql

import com.mysql.jdbc.Driver
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSqlMysqlSession {
  def main(args: Array[String]): Unit = {
    val sparkSqlMysqlSession: SparkSession = SparkSession.builder()
      .config("spark.master", "local[*]")
      .config("spark.sql.shuffle.partitions", "1")
      .appName("SparkSqlMysqlSession").getOrCreate()

    //sparkSession内部已经有了SparkContext
    //    sparkSqlMysqlSession.sparkContext

    //option应该根据format里指定的不同格式进行填写
    //最后必须执行一个load以产生spark-sql的数据操作对象df
    val data: DataFrame = sparkSqlMysqlSession.read.format("jdbc")
      .option("driver", classOf[Driver].getName)
      .option("url", "jdbc:mysql://nn2.hadoop:3306/hainiucralwer")
      .option("dbtable", "hainiu_web_seed")
      .option("user", "hainiu")
      .option("password", "12345678").load()
    data.createOrReplaceTempView("temp")
    val rows: DataFrame = sparkSqlMysqlSession.sql("select host from temp")
    rows.show()

  }

}
