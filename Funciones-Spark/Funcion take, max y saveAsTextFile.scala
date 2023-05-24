// Databricks notebook source
val sc = spark.sparkContext

// COMMAND ----------

val rdd = sc.parallelize("Isaias es GoshaDota".split(" "))

// COMMAND ----------

rdd.take(2)

// COMMAND ----------

val rddNumero = sc.parallelize(1 to 15)

// COMMAND ----------

rddNumero.max

// COMMAND ----------

rddNumero.min

// COMMAND ----------

rddNumero.saveAsTextFile("/dbfs/FileStore/lectura14")

// COMMAND ----------

dbutils.fs.ls("/dbfs/FileStore/lectura14")

// COMMAND ----------

rddNumero.getNumPartitions

// COMMAND ----------

rddNumero.coalesce(1).saveAsTextFile("/dbfs/FileStore/lectura14/rdd1")

// COMMAND ----------

dbutils.fs.ls("/dbfs/FileStore/lectura14/rdd1")