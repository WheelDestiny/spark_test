package com.wheelDestiny.Spark.hainiu


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}


class SparkWordCount(path:String){
  def deletePath = {
    val hadoopConf = new Configuration()
    val fs: FileSystem = FileSystem.get(hadoopConf)

    val outputPath = new Path(path)
    if(fs.exists(outputPath)){
      fs.delete(outputPath,true)
    }

  }
}

object SparkWordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //引入隐式转换给字符串赋予了删除HDFS路径的功能
    val outPath = "/Users/leohe/Data/output/wordcount"
    import com.wheelDestiny.Spark.Util.MyPredef.deletePath
    outPath.deletePath




  }




}
