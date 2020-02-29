package com.app

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object TerminalOps {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TerminalOps")
                              .setMaster("local[*]")
                              .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    val infomation = spark.read.parquet("file:///f:/data/obj/hx/parquet/part-00000-3bb8e5cd-72b7-4897-978d-4906485cf6d9-c000.snappy.parquet")

    infomation.rdd.map( row => {
      val ispname = row.getAs[String]("ispname")
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val winprice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")

      val requestNum = ResulUtil.requesCounttProcesser(requestmode,processnode)

      val clickNum = ResulUtil.clickCountProcesser(requestmode, iseffective)

      val admun = ResulUtil.admunCountProcesser(iseffective, isbilling, isbid, iswin, adorderid, winprice, adpayment)

      (ispname,requestNum++clickNum++admun)
    } ).reduceByKey( (v1,v2) => {
      v1.zip(v2).map(p => p._1+p._2)
    }).foreach(println)

  }

}
