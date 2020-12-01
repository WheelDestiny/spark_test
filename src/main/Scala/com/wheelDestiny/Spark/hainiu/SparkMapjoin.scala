package com.wheelDestiny.Spark.hainiu

import com.wheelDestiny.Spark.Util.{HainiuOrcNewOutputFormat, ORCFormat, ORCUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.ql.io.orc.{OrcNewInputFormat, OrcNewOutputFormat, OrcStruct}
import org.apache.hadoop.io.{NullWritable, Writable}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source

class SparkMapjoin{}
object SparkMapjoin {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("MapJoin").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val orcPath = "in/000000_0_copy_1"

    val hadoopConf = new Configuration()

    val orcFileRDD: RDD[(NullWritable, OrcStruct)] = sc.newAPIHadoopFile(orcPath, classOf[OrcNewInputFormat], classOf[NullWritable], classOf[OrcStruct], hadoopConf)

    val dictPath = "in/country_dict.dat"
    val list: List[String] = Source.fromFile(dictPath).getLines().toList

    import scala.collection.mutable.Map
    val map: mutable.Map[String, String] = Map[String, String]()

    list.foreach(l => {
      val strings: Array[String] = l.split("\t")
      val code: String = strings(0)
      val name: String = strings(1)
      map(code) = name
    })

    val broadCast: Broadcast[mutable.Map[String, String]] = sc.broadcast(map)
    val hasCountry = sc.longAccumulator
    val noHasCountry: LongAccumulator = sc.longAccumulator


    val value1: RDD[String] = orcFileRDD.mapPartitions(f => {
      val orcUtil = new ORCUtil
      orcUtil.setORCtype(ORCFormat.INS_STATUS)
      val strings = new ListBuffer[String]

      f.foreach(ff => {
        orcUtil.setRecord(ff._2)
        val countryMap: mutable.Map[String, String] = broadCast.value
        val countryCode: String = orcUtil.getData("country")
        val countryName: String = countryMap.getOrElse(countryCode, "")
        if (!"".equals(countryName)) {
          hasCountry.add(1)
          strings += s"${countryCode}\t${countryName}"
        } else {
          noHasCountry.add(1)
        }
      })
      strings.toIterator
    })


    //    val value: RDD[String] = orcFileRDD.map(f => {
    //      val orcUtil = new ORCUtil
    //      orcUtil.setORCtype(ORCFormat.INS_STATUS)
    //      orcUtil.setRecord(f._2)
    //      val countryMap: mutable.Map[String, String] = broadCast.value
    //      val countryCode: String = orcUtil.getData("country")
    //      val countryName: String = countryMap.getOrElse(countryCode, "")
    //      if (!"".equals(countryName)) {
    //        hasCountry.add(1)
    //      } else {
    //        noHasCountry.add(1)
    //      }
    //      s"${countryCode}\t${countryName}"
    //    })
    //    value.foreach(println)
    val orcOutRDD: RDD[(NullWritable, Writable)] = value1.mapPartitions(f => {
      val orcUtil = new ORCUtil
      orcUtil.setORCWriteType("struct<countryCode:string,countryName:string>")
      val rl = new ListBuffer[(NullWritable, Writable)]

      f.foreach(ff => {
        val strings: Array[String] = ff.split("\t")
        orcUtil.addAttr(strings(0)).addAttr(strings(1))
        rl += ((NullWritable.get(), orcUtil.serialize()))
      })
      rl.toIterator
    })

    import com.wheelDestiny.Spark.Util.MyPredef.deletePath
    val outPutPath = "outPut"
    outPutPath.deletePath

    hadoopConf.set("orc.compress","SNAPPY")
    hadoopConf.set("orc.create.index","true")

    orcOutRDD.saveAsNewAPIHadoopFile(outPutPath,classOf[NullWritable],classOf[Writable],classOf[HainiuOrcNewOutputFormat],hadoopConf)


    println(s"has${hasCountry.value}\tnohas${noHasCountry.value}")

  }


}
