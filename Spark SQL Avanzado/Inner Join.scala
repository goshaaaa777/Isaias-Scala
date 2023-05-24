// Databricks notebook source

val departamentos = spark.read.parquet("/FileStore/isaiasdata/scala/departamentoss.parquet")

val empleados = spark.read.parquet("/FileStore/isaiasdata/scala/empleadoss.parquet")


// COMMAND ----------

departamentos.show

// COMMAND ----------

empleados.show

// COMMAND ----------

import org.apache.spark.sql.functions.col

// COMMAND ----------

empleados.join(departamentos, col("num_dpto") === col("id"), "leftouter").show

// COMMAND ----------

empleados.join(departamentos, col("num_dpto") === col("id"), "left_outer").show

// COMMAND ----------

empleados.join(departamentos, col("num_dpto") === col("id"))

// COMMAND ----------

