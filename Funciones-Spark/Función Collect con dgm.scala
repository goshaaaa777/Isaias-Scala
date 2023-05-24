// Databricks notebook source
val sc = spark.sparkContext

// COMMAND ----------

val rdd = sc.parallelize("Hola spark con databricks".split(" "))

val arrayStrings = rdd.collect

// COMMAND ----------

arrayStrings(1)

// COMMAND ----------

val rrdNumero = sc.parallelize(1 to 10).flatMap( x => List(x, x * x))
rrdNumero.collect

// COMMAND ----------



// COMMAND ----------

