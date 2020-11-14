package com.wheelDestiny.Spark.core

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}

object Spark20_HbaseConnect {

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(TableInputFormat.INPUT_TABLE,"wheeldestiny:user")

//    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(hbaseConf,
//      classOf[TableInputFormat],
//      classOf[ImmutableBytesWritable],
//      classOf[Result]
//    )
//    hbaseRDD.foreach{
//      case (rowkey,result)=>{
//        val cells = result.rawCells()
//        for(cell<-cells){
//          println(Bytes.toString(CellUtil.cloneValue(cell)))
//        }
//      }
//    }
    val dataRDD = sc.makeRDD(List(("1002", "qian"), ("1003", "sun"), ("1004", "li")))
    val putRDD = dataRDD.map {
      case (rowkey, name) => {
        val put = new Put(Bytes.toBytes(rowkey))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(name))
        (new ImmutableBytesWritable(Bytes.toBytes(rowkey)), put)
      }
    }
    val jobConf =new JobConf(hbaseConf)
    //设置输出位置以及输出格式
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,"wheeldestiny:user")

    putRDD.saveAsHadoopDataset(jobConf)

    sc.stop()
  }

}
