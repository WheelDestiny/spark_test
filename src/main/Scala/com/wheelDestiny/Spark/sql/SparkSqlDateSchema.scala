package com.wheelDestiny.Spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, RowFactory, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object SparkSqlDateSchema {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSqlJson")
    val sc = new SparkContext(conf)

    val jsonPath = "in/user.json"

    val map: RDD[Row] = sc.textFile(jsonPath).map(f => RowFactory.create(f))

    //定义字段
    val fields = new ArrayBuffer[StructField]()
    fields += DataTypes.createStructField("line", DataTypes.StringType, true)
    //使用定义的字段类型创建了表结构
    val tableSchema: StructType = DataTypes.createStructType(fields.toArray)

    val sqlc = new SQLContext(sc)
    //使用自定义的表结构与Row类型的RDD进行结合生成指定表结构类型的DF
    val df: DataFrame = sqlc.createDataFrame(map, tableSchema)
    //    df.printSchema()
    //    df.show()

    //spark-sql的like的api用法
    //select line from table where line like '%zhao%'
    val filter: Dataset[Row] = df.filter(df.col("line").like("%zhao%"))
    filter.printSchema()
    filter.show()

    //统计
    val l: Long = filter.count()
    println(l)


  }

}
