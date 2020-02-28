package com.app

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object LocationOps {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("LocationOps")
                              .setMaster("local[*]")
                              .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    val session = SparkSession.builder().config(conf).getOrCreate()

    val dataframez = session.read.parquet("file:///f:/data/obj/hx/parquet/part-00000-3bb8e5cd-72b7-4897-978d-4906485cf6d9-c000.snappy.parquet")

    dataframez.createTempView("logs")
    val reuslt = session.sql(
      """
        |select
        | provincename,cityname,
        | sum(case when requestmode =1 and processnode >= 1 then 1 else 0 end) ysrequest,
        | sum(case when requestmode =1 and processnode >= 2 then 1 else 0 end) yxrequest,
        | sum(case when requestmode =1 and processnode = 3 then 1 else 0 end) adrequest,
        | sum(case when iseffective =1 and isbilling =1 and isbid =1 then 1 else 0 end) cybid,
        | sum(case when iseffective =1 and isbilling =1 and iswin =1 and adorderid !=0 then 1 else 0 end) cyAccbid,
        | sum(case when requestmode =2 and iseffective =1 then 1 else 0 end) shows,
        | sum(case when requestmode =3 and iseffective =1 then 1 else 0 end) clicks,
        | sum(case when iseffective =1 and isbilling =1 and iswin =1 then winprice/1000 else 0.0 end) pricost,
        | sum(case when iseffective =1 and isbilling =1 and iswin =1 then adpayment/1000 else 0.0 end) adpay
        |from
        | logs
        |group by
        | provincename,cityname
      """.stripMargin).show()
  }
}
