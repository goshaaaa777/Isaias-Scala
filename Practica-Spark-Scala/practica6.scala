// Databricks notebook source
//dbfs/FileStore/isaiasdata/scala/Case-1.csv
//dbfs/FileStore/isaiasdata/scala/PatientInfo-1.csv

// COMMAND ----------

val casosRow = spark.read.option("header","true").option("inferSchema","true").csv("/FileStore/isaiasdata/scala/Case-1.csv")

// COMMAND ----------

casosRow.printSchema

// COMMAND ----------

val casosNew = casosRow.withColumnRenamed(" case_id","case_id")

// COMMAND ----------

casosNew.printSchema

// COMMAND ----------

import org.apache.spark.sql.functions.{col, desc}

casosNew.orderBy(desc("confirmed"))
.select(
    col("province"),
    col("city"),
    col("confirmed")
)
.filter(col("city") =!= "-" and col("city") =!= "from other city")
.show(3, false)

// COMMAND ----------

val pacienteInfo = spark.read.option("header", "true").option("inferSchema","true").csv("/FileStore/isaiasdata/scala/PatientInfo-1.csv")

// COMMAND ----------

pacienteInfo.printSchema

// COMMAND ----------

pacienteInfo.count

// COMMAND ----------

pacienteInfo.select(col("patient_id")).distinct.count

// COMMAND ----------

val pacientesNew = pacienteInfo.dropDuplicates("patient_id")

pacientesNew.count

// COMMAND ----------

import org.apache.spark.sql.functions.count

pacientesNew.select(count(col("infected_by")).as("conteo")).show

// COMMAND ----------

display(pacientesNew)

// COMMAND ----------

val pacientesConInfoContagios = pacienteInfo.filter(col("infected_by").isNotNull)

pacientesConInfoContagios.count

// COMMAND ----------

val pacienteFemenino = pacientesConInfoContagios.filter(col("sex") === "female").drop("released_date", "deceased_date")

// COMMAND ----------

pacienteFemenino.printSchema

// COMMAND ----------

display(pacienteFemenino)

// COMMAND ----------

pacienteFemenino.rdd.getNumPartitions

// COMMAND ----------

pacienteFemenino.repartition(2).write.partitionBy("province").mode("overwrite").parquet("/FileStore/isaiasdata/scala/out")

// COMMAND ----------

dbutils.fs.ls("/FileStore/isaiasdata/scala/out")

// COMMAND ----------

