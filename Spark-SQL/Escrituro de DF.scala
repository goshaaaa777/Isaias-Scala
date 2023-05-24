// Databricks notebook source
val df = spark.read.parquet("/FileStore/isaiasdata/scala/datos.parquet")

// COMMAND ----------

df.select("title").show(5, false)

// COMMAND ----------

df.write.format("csv").option("Sep","|").save("/FileStore/sec7/Lectura50/csv")

// COMMAND ----------

dbutils.fs.ls("/FileStore/sec7/Lectura50/csv")

// COMMAND ----------

df.rdd.getNumPartitions

// COMMAND ----------

df.repartition(2).write.format("csv").option("sep","|").mode("overwrite").save("/FileStore/sec7/Lectura50/csv")

// COMMAND ----------

dbutils.fs.ls("/FileStore/sec7/Lectura50/csv")

// COMMAND ----------

df.printSchema

// COMMAND ----------

import org.apache.spark.sql.functions.col

// COMMAND ----------

df.select(col("comments_disabled")).distinct.show

// COMMAND ----------

val dfFiltrado = df.filter(col("comments_disabled").isin("False","True"))

// COMMAND ----------

dfFiltrado.write.partitionBy("comments_disabled").parquet("/FileStore/sec7/Lectura50/parquet")

// COMMAND ----------

dbutils.fs.ls("/FileStore/sec7/Lectura50/parquet")