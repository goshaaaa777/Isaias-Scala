// Databricks notebook source
import org.apache.spark.sql.functions.col

val departamentos = spark.read.parquet("/FileStore/isaiasdata/scala/departamentoss.parquet")

val empleados = spark.read.parquet("/FileStore/isaiasdata/scala/empleadoss.parquet")

departamentos.show
empleados.show

// COMMAND ----------

val empleadosRenombrado = empleados.withColumnRenamed("num_dpto","id")

// COMMAND ----------

empleadosRenombrado.show

// COMMAND ----------

val dfConColumnasDuplicadas = empleadosRenombrado.join(departamentos, empleadosRenombrado.col("id") === departamentos.col("id"))

// COMMAND ----------

dfConColumnasDuplicadas.show

// COMMAND ----------

dfConColumnasDuplicadas.select(
  empleadosRenombrado.col("id")
).show

// COMMAND ----------

val sinColDuplicado = empleadosRenombrado.join(departamentos, Seq("id"))

// COMMAND ----------

sinColDuplicado.show

// COMMAND ----------

sinColDuplicado.select(col("id")).show