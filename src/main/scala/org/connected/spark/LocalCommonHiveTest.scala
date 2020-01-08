package org.connected.spark

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object LocalCommonHiveTest {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .enableHiveSupport()
      .master("local")
      .appName("Demo")
      .config("spark.sql.warehouse.dir","C:/tmp/hive/spark-warehouse")
      .config("javax.jdo.option.ConnectionURL","jdbc:derby:;databaseName=C:/tmp/hive/metastore_db;create=true")
      .getOrCreate()

    spark.sql("SHOW DATABASES").show
    spark.sql("CREATE DATABASE IF NOT EXISTS sparkdemo")
    spark.sql("CREATE TABLE IF NOT EXISTS sparkdemo.table1(id INT, name STRING)")
    spark.sql("SHOW DATABASES").show

    import spark.implicits._
    val df:DataFrame = Seq(
      (1, "One"),
      (2, "Two"),
      (3, "Three")
    ).toDF("id","name")

    df.write.mode(SaveMode.Append).format("hive").saveAsTable("sparkdemo.table1")
    //Thread.sleep(60 * 1000)
    spark.sql("SELECT * FROM sparkdemo.table1").show(false)
    println(spark.sql("select * from sparkdemo.table1").count)

  }
}
