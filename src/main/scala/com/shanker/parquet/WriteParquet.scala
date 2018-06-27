package com.shanker.parquet

import org.apache.spark.sql.SparkSession

object WriteParquet {

  case class ParqueRecord(id: Int, data: String)

  def main(args: Array[String]): Unit = {

    val session = {
      SparkSession.builder()
        .appName("Parquet")
        .master("local[2]")
        .getOrCreate()
    }
    val sqlContext = session.sqlContext

    import sqlContext.implicits._
    val sc = session.sparkContext

    sc.parallelize(Range(1, 100)).map(x => ParqueRecord(x, x + "")).toDF.write.parquet("data1/")

  }
}