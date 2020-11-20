package com.wheelDestiny.Spark.sql

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

object SparkSqlHiveHql {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HainiuSqlData")
    conf.set("spark.shuffle.partitions", "1")
    val sc = new SparkContext(conf)

    val orcPath = "in/000000_0_copy_1"

    val hivec = new HiveContext(sc)

    hivec.sql("create database if not exists wheeldestiny")
    hivec.sql("use wheeldestiny")

    //创建一个orc内部表，数据存放在spark-warehouse
    val createTableSql =
      """
        |CREATE TABLE if not exists `user_install_status_spark`(
        |  `aid` string COMMENT 'from deserializer',
        |  `pkgname` string COMMENT 'from deserializer',
        |  `uptime` bigint COMMENT 'from deserializer',
        |  `type` int COMMENT 'from deserializer',
        |  `country` string COMMENT 'from deserializer',
        |  `gpcategory` string COMMENT 'from deserializer')
        |ROW FORMAT SERDE
        |  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
        |STORED AS INPUTFORMAT
        |  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
        |OUTPUTFORMAT
        |  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
        |TBLPROPERTIES ('orc.compress'='SNAPPY', 'orc.create.index'='true')
    """.stripMargin

    hivec.sql(createTableSql)
    hivec.sql(s"load data local inpath '${orcPath}' into table user_install_status_spark")

    val sql: DataFrame = hivec.sql(
      """
        |select concat(country,"\t",num) as concat_string from
        |(select country,count(1) as num from user_install_status_spark group by country) a
        |where a.num < 3 limit 2
        |""".stripMargin
    )
    val cache: sql.type = sql.cache()
    cache.show()
    cache.write.mode(SaveMode.Overwrite).format("text").save("outPut/sparkHive2Text")


  }

}
