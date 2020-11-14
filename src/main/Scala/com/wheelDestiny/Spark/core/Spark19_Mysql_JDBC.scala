package com.wheelDestiny.Spark.core

import org.apache.spark.{SparkConf, SparkContext}

object Spark19_Mysql_JDBC {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(conf)
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://nn2.hadoop:3306/hainiutest"
    val userName = "hainiu"
    val passWd = "12345678"

    //查询数据
    //一定要加上下限条件，为了能将数据分给多个reduce处理
//    val sql = "select id,goods_cname from wheelDestiny_goods_table where id>=? and id <=?"
//    //创建JDBCRDD
//    //访问数据库
//    val jdbcRDD: JdbcRDD[Unit] = new JdbcRDD(
//      sc,
//      () => {
//        //获取数据库连接对象
//        Class.forName(driver)
//        java.sql.DriverManager.getConnection(url, userName, passWd)
//      }, sql,
//      1, 3, 2,
//      (rs) => {
//        println(rs.getInt(1) + "," + rs.getString(2))
//      }
//    )
//    jdbcRDD.collect().foreach(println)
    val dataRDD = sc.makeRDD(List(("goodsn", "goodcname", "goodename", 11.1),
      ("goodsn", "goodcname", "goodename", 11.2),
      ("goodsn", "goodcname", "goodename", 11.3)))
    /*
    dataRDD.foreach{
      case(sn,cn,en,gp)=>{
        Class.forName(driver)
        val connection = java.sql.DriverManager.getConnection(url, userName, passWd)

        val sql = "insert into wheelDestiny_goods_table(goods_sn,goods_cname,goods_ename,goods_price) values (?,?,?,?)"
        val statement = connection.prepareStatement(sql)
        statement.setString(1,sn)
        statement.setString(2,cn)
        statement.setString(3,en)
        statement.setDouble(4,gp)
        statement.executeUpdate()
        statement.close()
        connection.close()
      }
    }
     */
    //分区间遍历
    dataRDD.foreachPartition(
      datas=>{
        Class.forName(driver)
        val connection = java.sql.DriverManager.getConnection(url, userName, passWd)

        datas.foreach{
          case(sn,cn,en,gp)=>{
            val sql = "insert into wheelDestiny_goods_table(goods_sn,goods_cname,goods_ename,goods_price) values (?,?,?,?)"
            val statement = connection.prepareStatement(sql)
            statement.setString(1,sn)
            statement.setString(2,cn)
            statement.setString(3,en)
            statement.setDouble(4,gp)
            statement.executeUpdate()
            statement.close()
            connection.close()
          }
        }
      }
    )
    sc.stop()
  }

}
