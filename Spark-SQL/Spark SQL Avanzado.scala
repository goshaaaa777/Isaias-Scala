// Databricks notebook source
/*
dbfs:/FileStore/isaiasdata/scala/muestra.parquet
dbfs:/FileStore/isaiasdata/scala/vuelos.parquet */
das

// COMMAND ----------

val dfMuestra = spark.read.parquet("/FileStore/isaiasdata/scala/muestra.parquet")
val dfVuelos = spark.read.parquet("/FileStore/isaiasdata/scala/vuelos.parquet")

// COMMAND ----------

dfMuestra.show

// COMMAND ----------

import org.apache.spark.sql.functions.{col, count, countDistinct, approx_count_distinct}

// COMMAND ----------

dfMuestra.select(
    count(col("nombre")).as("Conteo_Nombre"),
    count(col("color")).as("Conteo_Color")
).show

// COMMAND ----------

dfMuestra.select(
    count(col("nombre")).as("Conteo_Nombre"),
    count(col("color")).as("Conteo_Color"),
    count("*").as("Conteo_General")
).show

// COMMAND ----------

dfMuestra.select(
  countDistinct(col("color")).as("color_Df")
).show

// COMMAND ----------

dfVuelos.printSchema

// COMMAND ----------

display(dfVuelos)

// COMMAND ----------

dfVuelos.select(
  countDistinct(col("AIRLINE")).as("Conteo_Aerolineas"), 
  approx_count_distinct(col("AIRLINE")).as("Conteo_Aerolineas_Aprox")
).show

// COMMAND ----------



// COMMAND ----------

