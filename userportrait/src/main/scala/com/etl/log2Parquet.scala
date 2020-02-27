package com.etl

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

object log2Parquet {
  def main(args: Array[String]): Unit = {

//    if ( args.length != 2 ) {
//      println(
//        """
//          |usage ---> inputpath outpath
//          |""".stripMargin)
//      System.exit(0)
//    }
//
//    val Array( inputPath, outputPath ) = args
    val conf = new SparkConf().setMaster("local[*]")
                              .setAppName("log2Parquet")
                              .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    val session = SparkSession.builder().config(conf).getOrCreate()

    session.sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")

    val lines:RDD[String] = session.sparkContext.textFile("file:///f:/data/obj/hx/2016-10-01_06_p1_invalid.1475274123982.log")

    val info = lines.map( l => l.split(",", l.length)).filter(_.length >= 85).map( arr => {
      Row(
        arr(0),
        Str2Type.toInt(arr(1)),
        Str2Type.toInt(arr(2)),
        Str2Type.toInt(arr(3)),
        Str2Type.toInt(arr(4)),
        arr(5),
        arr(6),
        Str2Type.toInt(arr(7)),
        Str2Type.toInt(arr(8)),
        Str2Type.toDouble(arr(9)),
        Str2Type.toDouble(arr(10)),
        arr(11),
        arr(12),
        arr(13),
        arr(14),
        arr(15),
        arr(16),
        Str2Type.toInt(arr(17)),
        arr(18),
        arr(19),
        Str2Type.toInt(arr(20)),
        Str2Type.toInt(arr(21)),
        arr(22),
        arr(23),
        arr(24),
        arr(25),
        Str2Type.toInt(arr(26)),
        arr(27),
        Str2Type.toInt(arr(28)),
        arr(29),
        Str2Type.toInt(arr(30)),
        Str2Type.toInt(arr(31)),
        Str2Type.toInt(arr(32)),
        arr(33),
        Str2Type.toInt(arr(34)),
        Str2Type.toInt(arr(35)),
        Str2Type.toInt(arr(36)),
        arr(37),
        Str2Type.toInt(arr(38)),
        Str2Type.toInt(arr(39)),
        Str2Type.toDouble(arr(40)),
        Str2Type.toDouble(arr(41)),
        Str2Type.toInt(arr(42)),
        arr(43),
        Str2Type.toDouble(arr(44)),
        Str2Type.toDouble(arr(45)),
        arr(46),
        arr(47),
        arr(48),
        arr(49),
        arr(50),
        arr(51),
        arr(52),
        arr(53),
        arr(54),
        arr(55),
        arr(56),
        Str2Type.toInt(arr(57)),
        Str2Type.toDouble(arr(58)),
        Str2Type.toInt(arr(59)),
        Str2Type.toInt(arr(60)),
        arr(61),
        arr(62),
        arr(63),
        arr(64),
        arr(65),
        arr(66),
        arr(67),
        arr(68),
        arr(69),
        arr(70),
        arr(71),
        arr(72),
        Str2Type.toInt(arr(73)),
        Str2Type.toDouble(arr(74)),
        Str2Type.toDouble(arr(75)),
        Str2Type.toDouble(arr(76)),
        Str2Type.toDouble(arr(77)),
        Str2Type.toDouble(arr(78)),
        arr(79),
        arr(80),
        arr(81),
        arr(82),
        arr(83),
        Str2Type.toInt(arr(84))
      )
    })

     val frame = session.createDataFrame(info,SchemaUtils.logStructType)
    frame.createTempView("_sourceData")
    val localWriteFrame = frame.sqlContext.sql(
      """
        |select
        | count(*) as ct,
        | `provincename`,
        | `cityname`
        |from
        | `_sourceData`
        |group by
        | `provincename`,
        | `cityname`
        |""".stripMargin).show
//    localWriteFrame.coalesce(1).write.json("file:///f:/data/obj/hx/tongji")

    val url = "jdbc:mysql://localhost:3306/yonghuhuaxiang"
    val table = "user_spark_sql"
    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "root")
    frame.write.mode(SaveMode.Append).jdbc(url, table, properties)

    session.stop
  }
}
