// Databricks notebook source
val departamentos = spark.read.parquet("/FileStore/isaiasdata/scala/departamentoss.parquet")

val empleados = spark.read.parquet("/FileStore/isaiasdata/scala/empleadoss.parquet")


// COMMAND ----------

import org.apache.spark.sql.functions.{col, broadcast}

// COMMAND ----------

empleados.join(broadcast(departamentos), col("num_dpto") === col("id")).show

// COMMAND ----------

empleados.join(broadcast(departamentos), col("num_dpto") === col("id")).explain

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

