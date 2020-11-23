package com.wheelDestiny.Spark.hainiu.Hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, HTable, Put, Table}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

class SparkHBaseBuldLoad
object SparkHBaseBuldLoad {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("MapJoin")
//    conf.setMaster("local[*]")
    conf.set("spark.serializer", classOf[KryoSerializer].getName)

    val sc = new SparkContext(conf)

    import com.wheelDestiny.Spark.Util.MyPredef.deletePath
    //    val outPut = "outPut"
    val outPut = args(1)
    outPut.deletePath

    //    val orcPath = "in/000000_0_copy_1"
    val orcPath = args(0)

    val hivec = new HiveContext(sc)

    val df: DataFrame = hivec.read.orc(orcPath)
    val value: Dataset[Row] = df.limit(10)
    val rdd: RDD[Row] = value.rdd

    val hbaseRDD: RDD[(ImmutableBytesWritable, KeyValue)] = rdd.mapPartitions(rows => {
      val rl = new ListBuffer[(ImmutableBytesWritable, KeyValue)]

      for (row <- rows) {
        val pkg: String = row.getString(1)
        val country: String = row.getString(4)
        val keyValue = new KeyValue(Bytes.toBytes(s"spark_bulk_${pkg}"), Bytes.toBytes("info"), Bytes.toBytes("count"), Bytes.toBytes(country))
        val keyOut = new ImmutableBytesWritable()
        keyOut.set(Bytes.toBytes(s"spark_bulk_${pkg}"))

        rl += ((keyOut, keyValue))
      }
      rl.toIterator
    }).sortByKey()

    val hbaseConf: Configuration = HBaseConfiguration.create()
    val connection: Connection = ConnectionFactory.createConnection(hbaseConf)
    val table: HTable = connection.getTable(TableName.valueOf("wheeldestiny:user_install_statu")).asInstanceOf[HTable]
    val job: Job = Job.getInstance(hbaseConf)
    HFileOutputFormat2.configureIncrementalLoad(job, table.getTableDescriptor, table.getRegionLocator)

    hbaseRDD.saveAsNewAPIHadoopFile(outPut, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], hbaseConf)

    val strings: Array[String] = Array[String](outPut, "wheeldestiny:user_install_statu")

    LoadIncrementalHFiles.main(strings)

  }

}
