// Databricks notebook source
val sc = spark.sparkContext

// COMMAND ----------

val rdd = sc.parallelize(Seq(
  ("casa", 2),
  ("parque", 1),
  ("que",3),
  ("casa",5),
  ("escuela",3),
  ("casa",1),
  ("que",1)
  
))

// COMMAND ----------

rdd.collect

// COMMAND ----------

val rddReducido = rdd.reduceByKey(_ + _)

rddReducido.collect

// COMMAND ----------

val reddRedsucido2 = rdd.reduceByKey((x, y) => x + y)
reddRedsucido2.collect

// COMMAND ----------

