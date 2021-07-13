package com.btt.sparkWithJDBC

import org.apache.spark.sql.SparkSession
import java.util.Properties
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import com.btt.utils.constant

object dataSourceJdbc {

  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("SparkJDBC")
      .master("local[*]")
      .getOrCreate();

    val connectionProperties = new Properties();
    connectionProperties.setProperty("user", constant.USER_NAME);
    connectionProperties.setProperty("password", constant.PASS);

    /*
     * Read data from mysql table(countryinfo)
     */
    val mySqlDF = spark
      .read
      .jdbc(constant.JDBC_URL, constant.TABLE_NAME, connectionProperties);

    mySqlDF.printSchema();

    //Transformation
    val mysqlFilterData = mySqlDF.groupBy("Code", "Name")
      .agg(max("Population").as("Population"))
      .orderBy(col("Population").desc);

    mysqlFilterData.show();

    /*
     * Write data to mysql table(countryPopulation)
     */
    mysqlFilterData.write
      .mode(SaveMode.Overwrite)
      .option(
        s"${constant.SCHEMA_NAME}.countryPopulation",
        "Name VARCHAR(200),Code VARCHAR(100),Population int")
      .jdbc(
        constant.JDBC_URL,
        s"${constant.SCHEMA_NAME}.countryPopulation",
        connectionProperties);

    spark.close();

  }

}