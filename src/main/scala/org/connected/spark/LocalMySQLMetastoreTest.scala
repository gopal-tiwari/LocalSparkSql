package org.connected.spark

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object LocalMySQLMetastoreTest {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .enableHiveSupport()
      .master("local")
      .appName("Demo")
      .config("spark.sql.warehouse.dir", "C:\\tmp\\hive\\spark-warehouse")
      .config("javax.jdo.option.ConnectionURL", "jdbc:mysql://localhost:3306/metastore_db")
      .config("javax.jdo.option.ConnectionDriverName", "com.mysql.cj.jdbc.Driver")
      .config("javax.jdo.option.ConnectionUserName", "root")
      .config("javax.jdo.option.ConnectionPassword", "root")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    import spark.implicits._
    spark.sql("CREATE DATABASE IF NOT EXISTS sparkdemo")
    spark.sql(
      s"""
                  CREATE TABLE IF NOT EXISTS sparkdemo.table2
                  (
                    id INT,
                    name STRING
                  )
                  PARTITIONED BY(
                    date STRING
                  )
                  STORED AS PARQUET
                """)

    val df2: DataFrame = Seq(
      (4, "Four", "2020-01-13"),
      (5, "Five", "2020-01-13"),
      (6, "Six", "2020-01-15")
    ).toDF("id", "name", "date")
    df2.write.mode(SaveMode.Overwrite).insertInto("sparkdemo.table2")
    spark.sql("SELECT * FROM sparkdemo.table2").show
    spark.sql("SHOW PARTITIONS sparkdemo.table2").show
    spark.catalog.refreshTable("sparkdemo.table2")  // Should be used to see latest data added by other process
  }
}
