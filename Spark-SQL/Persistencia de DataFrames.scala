// Databricks notebook source
val df = spark.read.parquet("/FileStore/isaiasdata/scala/datos.parquet")

// COMMAND ----------

df.persist

// COMMAND ----------

df.unpersist

// COMMAND ----------

df.cache

// COMMAND ----------

df.unpersist

// COMMAND ----------

import org.apache.spark.storage.StorageLevel

// COMMAND ----------

df.persist(StorageLevel.DISK_ONLY)

// COMMAND ----------

df.unpersist

// COMMAND ----------

df.persist(StorageLevel.MEMORY_AND_DISK)

// COMMAND ----------

df.count