package com.util

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SaveMode}

object WriteData {
  def toParquet(frame: DataFrame) = {
    frame.coalesce(1).write.parquet("file:///f:/data/obj/hx/parquet")
  }

  def toMysql(frame: DataFrame) = {
//    val properties = new Properties()
//    properties.load( WriteData.getClass.getResourceAsStream("mysqlconn.properties") )
//    val mysqlurl  = properties.get("mysqlurl").toString
//    val sourceDataTable = properties.get("sourceDataTable").toString
//    val myuser = properties.get("myuser")
//    val passwd = properties.get("passwd")


    val stream = getClass.getClassLoader
    val config = ConfigFactory.load(stream,"myconn.properties")
//    val jdbcUrl = config.getString("mysqlurl")
//    val mytable = config.getString("sourceDataTable")
    val properties = new Properties()
    properties.put("user",config.getString("mysqlname"))
    properties.put("password",config.getString("password"))



//    val mysqlurl = "jdbc:mysql://localhost:3306/yonghuhuaxiang"
//    val table = "user_spark_sql"
//    val properties = new Properties()
//    properties.put("user","root")
//    properties.put("password","root")

    frame.write.mode(SaveMode.Append).jdbc(config.getString("mysql.url"),config.getString("target.table"),properties)
  }
//
//  def main(args: Array[String]): Unit = {
//    toMysql
//  }

}
