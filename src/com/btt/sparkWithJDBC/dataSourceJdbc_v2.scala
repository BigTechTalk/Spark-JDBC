package com.btt.sparkWithJDBC

import org.apache.spark.sql.SparkSession
import java.util.Properties
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import com.btt.utils.constant

/*
 * Using jdbc in a different way.
 */
object dataSourceJdbc_v2 {

  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("SparkJDBC")
      .master("local[*]")
      .getOrCreate();

    /*
     * Read data from mysql table(countryinfo)
     */
    val mySqlDF = spark
      .read.format("jdbc")
      .option("url", constant.JDBC_URL)
      .option("dbtable", constant.SCHEMA_NAME+"."+constant.TABLE_NAME)
      .option("user", constant.USER_NAME)
      .option("password", constant.PASS)
      .option("numPartions", 2)
      .load();

    mySqlDF.printSchema();

    //Transformation
    val mysqlFilterData = mySqlDF.groupBy("Code", "Name")
      .agg(max("Population").as("Population"))
      .orderBy(col("Population").desc);

    mysqlFilterData.show();

    /*
     * Write data to mysql table(countryPopulation)
     * Specifying create table column data types on write
     */
    mysqlFilterData.write
    .format("jdbc")
      .mode(SaveMode.Overwrite)
      .option(s"${constant.SCHEMA_NAME}.countryPopulation","Name VARCHAR(200),Code VARCHAR(100),Population int")
      .option("user", constant.USER_NAME)
      .option("password", constant.PASS)
      .option("url", constant.JDBC_URL)
      .option("dbtable", constant.SCHEMA_NAME+".countryPopulation")
      .save()


    spark.close();

  }

}