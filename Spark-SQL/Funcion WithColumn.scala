// Databricks notebook source
val df  = spark.read.parquet("/FileStore/isaiasdata/scala/datos.parquet")

// COMMAND ----------

import org.apache.spark.sql.functions.col

// COMMAND ----------

df.printSchema

// COMMAND ----------

val dfValoracion = df.withColumn("valoracion", col("likes") - col("dislikes"))

// COMMAND ----------

dfValoracion.printSchema

// COMMAND ----------

display(dfValoracion)

// COMMAND ----------

val dfValoracionCompleja = df.withColumn("valoracion", col("likes") - col("dislikes")).withColumn("resto", col("valoracion")% 10)

// COMMAND ----------

dfValoracionCompleja.printSchema

// COMMAND ----------

display(dfValoracionCompleja)

// COMMAND ----------

val dfRenombrado = df.withColumnRenamed("category_id","categoria")

// COMMAND ----------

dfRenombrado.printSchema

// COMMAND ----------



// COMMAND ----------

