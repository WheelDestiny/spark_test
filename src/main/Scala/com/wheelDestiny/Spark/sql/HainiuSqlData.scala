package com.wheelDestiny.Spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

class HainiuSqlData(private var line: String) {
  def getLine: String = {
    line
  }

  def setLine(line: String) = {
    this.line = line
  }
}

object HainiuSqlData {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HainiuSqlData")
    val sc = new SparkContext(conf)

    val jsonPath = "in/user.json"

    val map: RDD[HainiuSqlData] = sc.textFile(jsonPath).map(f => new HainiuSqlData(f))
    val sqlc = new SQLContext(sc)
    val df: DataFrame = sqlc.createDataFrame(map, classOf[HainiuSqlData])
    df.printSchema()
    df.show()

    df.createOrReplaceTempView("hainiu_table")

    val dff: DataFrame = sqlc.sql("select line from hainiu_table where line like '%zhao%'")

    val mapRDD: RDD[String] = dff.rdd.map(f => s"json_str:${f.getString(0)}")
    val strings: Array[String] = mapRDD.collect()
    strings.foreach(println)






  }

}
