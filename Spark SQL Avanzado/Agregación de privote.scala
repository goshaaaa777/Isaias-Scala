// Databricks notebook source
val estudiantes = spark.read.parquet("/FileStore/isaiasdata/scala/estudiantes.parquet")

// COMMAND ----------

estudiantes.printSchema

// COMMAND ----------

estudiantes.show

// COMMAND ----------

import org.apache.spark.sql.functions.{avg, col}

estudiantes.groupBy("graduacion").pivot("sexo").agg(avg(col("peso"))).show

// COMMAND ----------



// COMMAND ----------

