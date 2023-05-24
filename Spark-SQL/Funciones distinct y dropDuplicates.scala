// Databricks notebook source
val df = spark.read.parquet("/FileStore/isaiasdata/scala/data.parquet")

// COMMAND ----------

df.show

// COMMAND ----------

df.dropDuplicates("id", "color").show

// COMMAND ----------

df.distinct.show

// COMMAND ----------



// COMMAND ----------

