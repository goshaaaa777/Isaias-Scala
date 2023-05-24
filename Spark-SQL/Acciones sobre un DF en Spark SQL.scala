// Databricks notebook source
val df = spark.read.parquet("/FileStore/isaiasdata/scala/datos.parquet")

// COMMAND ----------

df.select("title").show(5, false)

// COMMAND ----------

df.head(1)

// COMMAND ----------

df.take(1)

// COMMAND ----------

df.select("likes").collect

// COMMAND ----------

df.count

// COMMAND ----------

