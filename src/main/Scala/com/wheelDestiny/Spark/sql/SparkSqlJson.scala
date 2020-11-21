package com.wheelDestiny.Spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkSqlJson {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSqlJson")
    val sc = new SparkContext(conf)

    val jsonPath = "in/user.json"

    val sqlc = new SQLContext(sc)
    val df: DataFrame = sqlc.read.json(jsonPath)

//    df.printSchema()
    df.createOrReplaceTempView("users")
    sqlc.sql("select * from users").show()
    //    df.show()
    val count: DataFrame = df.groupBy("name").count()
//    count.printSchema()
//    count.show()

    val rdd: RDD[Row] = count.rdd

    import com.wheelDestiny.Spark.Util.MyPredef.deletePath

    val outPut = "outPut"
    outPut.deletePath
    rdd.saveAsTextFile(outPut)



  }

}
