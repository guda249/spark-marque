package com.shanker.parquet

import org.apache.spark.sql.SparkSession

object ReadParquet {

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

    val parqueData = session.read.parquet("data1/")

    // registering as table for querying it
    parqueData.createOrReplaceTempView("data")
    session.table("data")
    
    // Reads the Schema
    println(parqueData.schema)

    // The prints the data that is read from the parquet
    // parqueData.show(100)

    // Querying the Parquet Data that is registered as table
    val resultData = session.sql(s"select * from data where id > 60")

    // Results
    resultData.foreach(println(_))

  }
}