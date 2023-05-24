// Databricks notebook source
val vuelos = spark.read.parquet("/FileStore/isaiasdata/scala/vuelos.parquet")

// COMMAND ----------

import org.apache.spark.sql.functions.{desc, col, avg, asc}

// COMMAND ----------

vuelos.printSchema

// COMMAND ----------

vuelos.groupBy("ORIGIN_AIRPORT").count.orderBy(asc("count")).show

// COMMAND ----------

vuelos.groupBy("ORIGIN_AIRPORT","DESTINATION_AIRPORT").count.orderBy(desc("count")).show

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

