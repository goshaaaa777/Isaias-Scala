// Databricks notebook source
val df = spark.read.parquet("/FileStore/isaiasdata/scala/strings.parquet")

// COMMAND ----------

df.printSchema

// COMMAND ----------

df.show(false)

// COMMAND ----------

import org.apache.spark.sql.functions.{col,trim, ltrim, rtrim}

// COMMAND ----------

df.select(
  col("nombre"),
  ltrim(col("nombre")).as("lt"),
  rtrim(col("nombre")).as("rt"),
  trim(col("nombre")).as("tr")
).show(false)

// COMMAND ----------

import org.apache.spark.sql.functions.{lpad, rpad}

// COMMAND ----------

df.select(
  col("nombre"),
  lpad(trim(col("nombre")),8, "-").as("lp"),
  rpad(trim(col("nombre")),8, "=").as("rp")
).show(false)

// COMMAND ----------

import org.apache.spark.sql.functions.{concat_ws, lower, upper, initcap, reverse}

// COMMAND ----------

df.select(
  concat_ws(" ", col("sujeto"),col("verbo"), col("adjetivo")).as("frase")
).select(
  col("frase"),
  lower(col("frase")).as("lw"),
  upper(col("frase")).as("up"),
  initcap(col("frase")).as("ic"),
  reverse(col("frase"))
).show(false)

// COMMAND ----------

import org.apache.spark.sql.functions.regexp_replace

// COMMAND ----------

df.select(
  concat_ws(" ",col("sujeto"), col("verbo"), col("adjetivo")).as("frase")
).select(
  col("frase"),
  regexp_replace(col("frase"),"Spark|es","lindo").as("regexp")
).show(false)

// COMMAND ----------

