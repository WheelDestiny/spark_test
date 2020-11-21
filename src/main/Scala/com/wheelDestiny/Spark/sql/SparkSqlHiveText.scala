package com.wheelDestiny.Spark.sql

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkSqlHiveText {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HainiuSqlData")
    conf.set("spark.shuffle.partitions", "1")
    val sc = new SparkContext(conf)

    val orcPath = "in/000000_0_copy_1"

    val hivec = new HiveContext(sc)
    val df: DataFrame = hivec.read.orc(orcPath)

    df.createOrReplaceTempView("user_install_status")

    //保存成text格式，但是text不支持多字段，所以要在sql上进行多字段拼接成一个字段
    val sql: DataFrame = hivec.sql(
      """
        |select concat(country,"\t",num) as concat_string from
        |(select country,count(1) as num from user_install_status group by country) a
        |where a.num < 3 limit 2
        |""".stripMargin)
    //spark-sql中的cache和RDD中的Cache一样，里面都是调用了presist
    val cache: sql.type = sql.cache()
    cache.show()
    cache.write.mode(SaveMode.Overwrite).format("text").save("outPut/sparkHive2Text")


  }

}
