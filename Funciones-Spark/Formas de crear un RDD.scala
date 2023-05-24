// Databricks notebook source
val sc = spark.sparkContext

// COMMAND ----------

val rddVacio = sc.emptyRDD

// COMMAND ----------

val rddVacio1 = sc.parallelize(Seq(),3)

// COMMAND ----------

rddVacio.getNumPartitions

// COMMAND ----------

rddVacio1.getNumPartitions

// COMMAND ----------

val rdd = sc.parallelize(Seq(1,2,3,4,5))

// COMMAND ----------

rdd.collect

// COMMAND ----------

val rddtexto = sc.textFile("dbfs:/FileStore/Seccion3/lectura12/rdd_source.txt")

// COMMAND ----------

rddtexto.collect

// COMMAND ----------

rdd.collect

// COMMAND ----------

val rddSuma = rdd.map(x => x + 1)

rddSuma.collect

// COMMAND ----------

import spark.implicits

// COMMAND ----------

val  df = Seq((1,"jk"),(2,"gh"),(3,"ga")).toDF("id","letras")
df.show

// COMMAND ----------

val rddDataFrame = df.rdd

// COMMAND ----------

rddDataFrame.collect