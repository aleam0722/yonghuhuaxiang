package com.app

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object AppOps {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]")
                              .setAppName("AppOps")
                              .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    val spark = SparkSession.builder().config(conf).getOrCreate()
/*1	乐自游	A06		cn.net.inch.android	通过GPS的定位实现景区的自动语音讲解的功能。<divclassbase-info>*/
    val dictMap = spark.sparkContext.textFile("file:///f:/data/obj/hx/app_dict.txt")
                      .map(_.split("\t",-1))
                      .filter(_.length>=5)
                      .map(arr => (arr(4),arr(1)))
                      .collectAsMap()

    val broadedDicMap = spark.sparkContext.broadcast(dictMap)

    val log = spark.read.parquet("file:///f:/data/obj/hx/parquet/part-00000-3bb8e5cd-72b7-4897-978d-4906485cf6d9-c000.snappy.parquet")

    log.rdd.map( row => {
      var appname = row.getAs[String]("appname")

      if (!StringUtils.isNoneBlank(appname)){
        appname = broadedDicMap.value.getOrElse(row.getAs[String]("appid"),"其他")

    }
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

      (appname,requestNum++clickNum++admun)
    }).reduceByKey((v1,v2)=> {
      v1.zip(v2).map(p => p._1+p._2)
    }).foreach(println)
  }

}
