// Databricks notebook source
val dfParquet = spark.read.parquet("/FileStore/isaiasdata/scala/datos.parquet")

// COMMAND ----------

import org.apache.spark.sql.functions.col

// COMMAND ----------

dfParquet.printSchema

// COMMAND ----------

dfParquet.select(col("video_id")).show

// COMMAND ----------

dfParquet.select("video_id","title").show

// COMMAND ----------

dfParquet.select(
  col("likes"),
  col("dislikes"),
  (col("likes") - col("dislikes")).as("aceptacion")
).show(false)

// COMMAND ----------

dfParquet.selectExpr("likes","dislikes","(likes - dislikes) as aceptacion").show(false)

// COMMAND ----------

