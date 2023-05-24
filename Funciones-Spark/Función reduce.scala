// Databricks notebook source
val sc = spark.sparkContext

// COMMAND ----------

val rdd = sc.parallelize(1 to 10)

// COMMAND ----------

rdd.reduce(_ + _)

// COMMAND ----------

10*11/2

// COMMAND ----------

val rddP = sc.parallelize(1 to 3)
rddP.reduce(_ * _)

// COMMAND ----------

)