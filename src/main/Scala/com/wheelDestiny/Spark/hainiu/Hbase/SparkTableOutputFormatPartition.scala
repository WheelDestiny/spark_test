package com.wheelDestiny.Spark.hainiu.Hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil
import org.apache.hadoop.io.NullWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object SparkTableOutputFormatPartition {


  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("MapJoin").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val orcPath = "in/000000_0_copy_1"

    val hivec = new HiveContext(sc)

    val df: DataFrame = hivec.read.orc(orcPath)

    df.createOrReplaceTempView("hainiu")

    val frame: DataFrame = hivec.sql("select pkgname,count(1) from hainiu group by pkgname")
    val rdd: RDD[Row] = frame.rdd

    val reRDD: RDD[Row] = rdd.repartition(300)

    val mapRDD: RDD[(NullWritable, Put)] = reRDD.mapPartitions(t => {
      val value = new ListBuffer[(NullWritable, Put)]
      for (next <- t) {
        val pkg: String = next.getString(0)
        val pkgCount: Long = next.getLong(1)
        val put = new Put(Bytes.toBytes(s"spark_tab_par_${pkg}"))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("count"), Bytes.toBytes(pkgCount))
        value += ((NullWritable.get(), put))
      }
      value.toIterator
    })

    val configuration: Configuration = HBaseConfiguration.create()
    configuration.set("mapreduce.job.outputformat.class",classOf[TableOutputFormat[NullWritable]].getName)
    configuration.set("mapreduce.job.key.class",classOf[NullWritable].getName)
    configuration.set("mapreduce.job.value.class",classOf[Put].getName)
    configuration.set("hbase.mapred.outputtable","wheeldestiny:SparkTest")
    val value: RDD[(NullWritable, Put)] = mapRDD.coalesce(5)

    value.saveAsNewAPIHadoopDataset(configuration)

  }

}
