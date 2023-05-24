// Databricks notebook source
val vuelos = spark.read.parquet("/FileStore/isaiasdata/scala/vuelos.parquet")

// COMMAND ----------

vuelos.printSchema

// COMMAND ----------

import org.apache.spark.sql.functions.{count,min,max,desc,avg}

// COMMAND ----------

vuelos.groupBy("ORIGIN_AIRPORT").agg(
  count("AIR_TIME").as("Tiempo_Aire"),
  min("AIR_TIME").as("Tiempo_minimop"),
  max("AIR_TIME").as("Tiempo_maximo")
).orderBy(desc("Tiempo_Aire")).show

// COMMAND ----------

vuelos.groupBy("MONTH").agg(
  count("ARRIVAL_DELAY").as("conteo_retraso"),
  avg("DISTANCE").as("promedio_distancia")
).orderBy("conteo_retraso").show

// COMMAND ----------

