package com.wheelDestiny.Spark.hainiu

import com.wheelDestiny.Spark.Util.ORCUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}

object SparkMapJoinKyro {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("MapJoin").setMaster("local[*]")
    conf.set("spark.serializer",classOf[KryoSerializer].getName)
    val sc = new SparkContext(conf)

    val util = new ORCUtil
    val bUtil: Broadcast[ORCUtil] = sc.broadcast(util)

    val intRDD: RDD[Int] = sc.parallelize(List(1, 0, 2, 3, 4, 5, 5))

    val orcRDD: RDD[(ORCUtil, Int)] = intRDD.map(f => {
      val util1: ORCUtil = bUtil.value
      (new ORCUtil, 1)
    })
    val value: RDD[(ORCUtil, Iterable[Int])] = orcRDD.groupByKey()
    value.take(200).foreach(println)




  }

}
