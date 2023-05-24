// Databricks notebook source
val vuelos = spark.read.parquet("/FileStore/isaiasdata/scala/vuelos.parquet")

// COMMAND ----------

vuelos.printSchema

// COMMAND ----------

import org.apache.spark.sql.functions.{col, sum, sumDistinct, avg, count}

// COMMAND ----------

//sum all
vuelos.select(
  sum(col("DISTANCE")).as("Distacia")
).show

// COMMAND ----------

//sum regular
vuelos.select(
  sumDistinct(col("DISTANCE")).as("Distacia_diff")
).show

// COMMAND ----------

vuelos.select(
  avg(col("AIR_TIME")).as("promedio"),
  (sum(col("AIR_TIME")) / count(col("AIR_TIME"))).as("Promedio Manuel")
).show

// COMMAND ----------

