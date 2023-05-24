// Databricks notebook source
// MAGIC %python
// MAGIC
// MAGIC from pyspark.sql import *
// MAGIC from pyspark.sql.functions import *
// MAGIC from pyspark import SparkContext
// MAGIC import pandas as pd
// MAGIC
// MAGIC # create the Spark Session
// MAGIC spark = SparkSession.builder.getOrCreate()
// MAGIC
// MAGIC # create the Spark Context
// MAGIC sc = spark.sparkContext

// COMMAND ----------

// MAGIC %python
// MAGIC trips = [
// MAGIC     (1, 1, 1, 1, '20160101', 10),
// MAGIC     (2, 2, 2, 2, '20160202', 20),
// MAGIC     (1, 1, 3, 1, '20160402', 15),
// MAGIC     (1, 1, 4, 3, '20160405', 20),
// MAGIC     (2, 2, 5, 4, '20160410', 25),
// MAGIC     (3, 3, 6, 3, '20160415', 15),
// MAGIC     (2, 2, 7, 1, '20160420', 40),
// MAGIC     (3, 3, 8, 2, '20160505', 80)
// MAGIC ]

// COMMAND ----------

