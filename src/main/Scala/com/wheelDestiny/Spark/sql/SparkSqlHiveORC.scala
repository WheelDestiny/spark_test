package com.wheelDestiny.Spark.sql

import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkSqlHiveORC {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HainiuSqlData")
    conf.set("spark.shuffle.partitions", "1")
    val sc = new SparkContext(conf)

    val orcPath = "in/000000_0_copy_1"


    val hivec = new HiveContext(sc)
    val df: DataFrame = hivec.read.orc(orcPath)
    //    df.printSchema()
    //    df.show()

    //    df.select(df.col("country")).show(5)

    //    df.select(df.col("country").as("local")).show(5)

    val count: DataFrame = df.groupBy("country").count()
    //    count.printSchema()
    //    count.show()

    val select: DataFrame = count.select(count.col("country"), count.col("count").alias("nums"))
    //    select.printSchema()
    //    select.show()
    val selectCount: Dataset[Row] = select.filter(select.col("nums").lt(3)).limit(10)
    selectCount.printSchema()
    selectCount.show()

    //DS的默认存储级别是MEMORY_AND_DISK
    val cache: selectCount.type = selectCount.persist()

    cache.write.mode(SaveMode.Overwrite).format("orc").save("outPut/sqlHive2Orc")
    cache.write.mode(SaveMode.Overwrite).format("json").save("outPut/sqlHive2Json")

  }

}
