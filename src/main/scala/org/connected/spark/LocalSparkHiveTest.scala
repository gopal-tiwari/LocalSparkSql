package org.connected.spark

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object LocalSparkHiveTest {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .enableHiveSupport()
      .master("local")
      .appName("Demo")
      .getOrCreate()

    spark.sql("SHOW DATABASES").show
    spark.sql("CREATE DATABASE IF NOT EXISTS sparkdemo")
    spark.sql("CREATE TABLE IF NOT EXISTS sparkdemo.table1(id INT, name STRING)")


  }
}
