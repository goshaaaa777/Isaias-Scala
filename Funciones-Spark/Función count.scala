// Databricks notebook source
val sc = spark.sparkContext

// COMMAND ----------

val rdd = sc.parallelize(Seq("j","o","d","e"))

// COMMAND ----------

rdd.count

// COMMAND ----------

val rddNumerico = sc.parallelize(1 to 70)
rddNumerico.count

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

