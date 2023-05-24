// Databricks notebook source
val df = spark.read.parquet("/FileStore/isaiasdata/scala/dataPARQUET.parquet")

// COMMAND ----------

df.printSchema

// COMMAND ----------

df.select("title").show(false)

// COMMAND ----------

import org.apache.spark.sql.functions.{col, column}

// COMMAND ----------

df.select(col("title")).show(false)

// COMMAND ----------

df.select(column("title")).show(false)

// COMMAND ----------

df.select($"title").show(false)

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

