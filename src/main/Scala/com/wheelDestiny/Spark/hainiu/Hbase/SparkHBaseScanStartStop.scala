package com.wheelDestiny.Spark.hainiu.Hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableMapReduceUtil}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkHBaseScanStartStop {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("MapJoin").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val configuration: Configuration = HBaseConfiguration.create()

    val scan = new Scan()
    scan.addFamily(Bytes.toBytes("info"))
    //每次从服务端拉取的数据量
    scan.setCaching(10000)
    //本次scan不向读缓存中存入数据
    scan.setCacheBlocks(false)
    scan.setStartRow(Bytes.toBytes("spark_B"))
    scan.setStopRow(Bytes.toBytes("spark_C"))

    configuration.set(TableInputFormat.SCAN,TableMapReduceUtil.convertScanToString(scan))

    configuration.set(TableInputFormat.INPUT_TABLE,"wheeldestiny:SparkTest")

    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(configuration, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    hbaseRDD.foreach(f=>{
      val cf: Array[Byte] = Bytes.toBytes("info")
      val q: Array[Byte] = Bytes.toBytes("count")
      val rowkey: String = Bytes.toString(f._1.get())

      val count: Int = Bytes.toInt(f._2.getValue(cf, q))

      println(s"rowKey:${rowkey}\tcount${count}")
    })




  }

}
