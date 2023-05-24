// Databricks notebook source
val sc = spark.sparkContext

// COMMAND ----------

val rdd = sc.parallelize(1 to 10, 5)

// COMMAND ----------

val rdd7 = rdd.repartition(7)
rdd7.getNumPartitions

// COMMAND ----------

val rdd3 = rdd.repartition(3)
rdd3.getNumPartitions

// COMMAND ----------



// COMMAND ----------

