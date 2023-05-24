// Databricks notebook source
val vuelos = spark.read.parquet("/FileStore/isaiasdata/scala/vuelos.parquet")

// COMMAND ----------

vuelos.printSchema

// COMMAND ----------

import org.apache.spark.sql.functions.{col, min, max}

// COMMAND ----------

vuelos.select(
  min(col("AIR_TIME").as("min_aire")),
  max(col("AIR_TIME").as("max_aire"))
).show

// COMMAND ----------

vuelos.describe("AIR_TIME").show

// COMMAND ----------

vuelos.select(
  min(col("AIRLINE_DELAY")).as("min_delay"),
  max(col("AIRLINE_DELAY")).as("max_delay")
).show

// COMMAND ----------

