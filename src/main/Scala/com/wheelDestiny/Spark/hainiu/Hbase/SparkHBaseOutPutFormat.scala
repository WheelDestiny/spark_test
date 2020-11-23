package com.wheelDestiny.Spark.hainiu.Hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.NullWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkHBaseOutPutFormat {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("MapJoin").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val unit: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9))
    val mapRDD: RDD[(NullWritable, Put)] = unit.map(f => {
      val put = new Put(Bytes.toBytes(s"spark_Tab_${f}"))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("count"), Bytes.toBytes(f))
      (NullWritable.get(), put)
    })

    val configuration: Configuration = HBaseConfiguration.create()
    configuration.set("mapreduce.job.outputformat.class",classOf[TableOutputFormat[NullWritable]].getName)
    configuration.set("mapreduce.job.key.class",classOf[NullWritable].getName)
    configuration.set("mapreduce.job.value.class",classOf[Put].getName)
    configuration.set("hbase.mapred.outputtable","wheeldestiny:SparkTest")

    mapRDD.saveAsNewAPIHadoopDataset(configuration)
  }

}
