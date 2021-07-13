package com.btt.utils

object constant {
  
    val SCHEMA_NAME = "countrydata";
    val TABLE_NAME ="countryinfo";
    val USER_NAME = "root";
    val PASS = "root";
    val JDBC_URL = s"jdbc:mysql://localhost:3306/${SCHEMA_NAME}?serverTimezone=UTC";
  
}