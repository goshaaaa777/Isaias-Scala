// Databricks notebook source
/*
dbfs:/FileStore/isaiasdata/scala/convertir.parquet
dbfs:/FileStore/isaiasdata/scala/calculo.parquet
*/


// COMMAND ----------

val df = spark.read.parquet("/FileStore/isaiasdata/scala/convertir.parquet")

// COMMAND ----------

df.printSchema

// COMMAND ----------

df.show(false)

// COMMAND ----------

import org.apache.spark.sql.functions.{col, to_date, to_timestamp, date_format}

// COMMAND ----------

val dfConvertido = df.select(
  to_date(col("date")).as("date1"),
  to_timestamp(col("timestamp")).as("timestamp1"),
  to_date(col("date"),"yyyy-MMM-dd").as("date2"),
  to_timestamp(col("timestamp"),"yyyy-MMM-dd HH:mm:ss.SSS").as("timestamp2")
)


// COMMAND ----------

dfConvertido.printSchema

// COMMAND ----------

dfConvertido.show(false)

// COMMAND ----------

dfConvertido.select(date_format(col("date1"),"MM-dd-yyyy")).show(false)

// COMMAND ----------

val data = spark.read.parquet("/FileStore/isaiasdata/scala/calculo.parquet")

// COMMAND ----------

data.printSchema

// COMMAND ----------

display(data)

// COMMAND ----------

import org.apache.spark.sql.functions.{datediff, months_between, last_day}

// COMMAND ----------

data.select(
  col("nombre"),
  datediff(col("fecha_salida"), col("fecha_ingreso")).as("dias"),
  months_between(col("fecha_salida"), col("fecha_ingreso")).as("meses"),
  last_day(col("fecha_salida")).as("ultimo_dia_mes")
).show(false)

// COMMAND ----------



// COMMAND ----------

