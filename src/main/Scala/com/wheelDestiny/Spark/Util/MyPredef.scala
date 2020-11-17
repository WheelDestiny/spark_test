package com.wheelDestiny.Spark.Util
import com.wheelDestiny.Spark.hainiu.SparkWordCount


object MyPredef {
  implicit def deletePath(path: String) = new SparkWordCount(path)

  implicit val a = new Ordering[(String,Int)](){
    override def compare(x: (String, Int), y: (String, Int)) = {
      (x._2 - y._2) * -1
    }
  }
}
