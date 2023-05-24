// Databricks notebook source
/*
dbfs:/FileStore/isaiasdata/scala/formato_json.parquet
dbfs:/FileStore/isaiasdata/scala/formato_array.parquet 
*/

// COMMAND ----------

val dfJson = spark.read.parquet("/FileStore/isaiasdata/scala/formato_json.parquet")
val dfArray = spark.read.parquet("/FileStore/isaiasdata/scala/formato_array.parquet")

// COMMAND ----------

dfArray.printSchema

// COMMAND ----------

dfJson.printSchema

// COMMAND ----------

import org.apache.spark.sql.functions.{col, size, sort_array, array_contains, explode}

// COMMAND ----------

dfArray.select(
  size(col("tareas")).as("longitud"),
  sort_array(col("tareas")).as("arr_ordenado"),
  array_contains(col("tareas"),"buscar agua").as("buscar_agua")
).show(false)

// COMMAND ----------

dfArray.select(
  col("dia"),
  explode(col("tareas")).as("tareas")
).show(false)

// COMMAND ----------

dfJson.printSchema

// COMMAND ----------

dfJson.show(false)

// COMMAND ----------

import org.apache.spark.sql.types.{StructType, StructField, StringType, ArrayType}

// COMMAND ----------

val SchemaJson = StructType(Array(
  StructField("dia", StringType, true),
  StructField("tareas", ArrayType(StringType), true)

))

// COMMAND ----------

import org.apache.spark.sql.functions.{from_json, to_json}

// COMMAND ----------

val dfJSON = dfJson.select(from_json(col("tareas_str"), schemaJson).as("por_hacer"))