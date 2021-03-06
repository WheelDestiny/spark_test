package com.wheelDestiny.Spark.hainiu.Hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object SparkHbaseTableBatchPut {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("MapJoin").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val unit: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9))
    //wheeldestiny:SparkTest
    unit.foreachPartition(f=>{
      val configuration: Configuration = HBaseConfiguration.create()
      val connection: Connection = ConnectionFactory.createConnection(configuration)
      val table: Table = connection.getTable(TableName.valueOf("wheeldestiny:SparkTest"))

      val putList =new ListBuffer[Put]

      for (next <- f) {
        val put = new Put(Bytes.toBytes(s"spark_Batch_${next}"))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("count"), Bytes.toBytes(next))
        putList += put
      }
      import scala.collection.convert.wrapAll._
      table.put(putList)

      table.close()
      connection.close()
    })

  }

}
