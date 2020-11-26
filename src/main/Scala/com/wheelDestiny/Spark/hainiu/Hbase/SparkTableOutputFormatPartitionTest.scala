package com.wheelDestiny.Spark.hainiu.Hbase

import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.NullWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object SparkTableOutputFormatPartitionTest {


  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("MapJoin").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val orcPath = "in/000000_0_copy_1"

    val hivec = new HiveContext(sc)

    val df: DataFrame = hivec.read.orc(orcPath)

    val rdd: RDD[Row] = df.rdd

    val mapRDD: RDD[(String, String)] = rdd.map(row => {
      (row(0).toString, row(4).toString)
    })
//    val value: RDD[(String, Int)] = mapRDD.groupByKey().map(f => {
//      (f._2.head, f._1)
//    }
//    ).groupByKey().map(f => {
//      (f._1, f._2.size)
//    }
//    )
//
//    val tuples1: Array[(String, Int)] = value.collect()

//    tuples1.foreach(println)

    val value1: RDD[(String, Int)] = mapRDD.reduceByKey((i, j) => j).mapPartitions(f => {
      val rl: ArrayBuffer[(String, Int)] = new ArrayBuffer[(String, Int)]
      for (kv <- f) {
        rl += ((kv._2, 1))
      }
      rl.toIterator
    }).reduceByKey(_+_)

    value1.collect().foreach(println)



  }

}
