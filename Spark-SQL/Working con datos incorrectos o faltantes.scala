// Databricks notebook source
val df = spark.read.parquet("/FileStore/isaiasdata/scala/datos.parquet")

// COMMAND ----------

df.count

// COMMAND ----------

df.na.drop.count

// COMMAND ----------

df.na.drop("any").count

// COMMAND ----------

df.na.drop("all").count

// COMMAND ----------

df.na.drop(Array("views")).count

// COMMAND ----------

df.na.drop(Seq("views","dislikes")).count

// COMMAND ----------

import org.apache.spark.sql.functions.col

// COMMAND ----------

df.orderBy(col("views")).select(col("views"),col("likes"),col("dislikes")).show

// COMMAND ----------

df.na.fill(7).orderBy(col("views")).select(col("views"),col("likes"),col("dislikes")).show

// COMMAND ----------

df.na.fill(0, Array("views", "likes")).orderBy(col("views")).select(col("views"),col("likes"),col("dislikes")).show

// COMMAND ----------

df.describe("likes").show