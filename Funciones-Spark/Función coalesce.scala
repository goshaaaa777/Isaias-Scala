// Databricks notebook source
val sc = spark.sparkContext

// COMMAND ----------

val rdd = sc.parallelize(1 to 10 , 10)

// COMMAND ----------

rdd.getNumPartitions

// COMMAND ----------

val rdd5 = rdd.coalesce(5)

// COMMAND ----------

rdd5.getNumPartitions

// COMMAND ----------

val rdd1 = rdd5.coalesce(1)
rdd1.getNumPartitions

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

