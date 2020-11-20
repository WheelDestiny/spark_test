package com.wheelDestiny.Spark

import com.wheelDestiny.Spark.hainiu.Hbase.SparkHBaseBuldLoad
import com.wheelDestiny.Spark.hainiu.SparkMapjoin
import org.apache.hadoop.util.ProgramDriver

object Driver {
  def main(args: Array[String]): Unit = {
    val driver = new ProgramDriver()
    driver.addClass("MapJoin",classOf[SparkMapjoin],"MapJoinTask")
    driver.addClass("SparkHBase",classOf[SparkHBaseBuldLoad],"Spark生成HFile文件")
    driver.run(args)
  }

}
