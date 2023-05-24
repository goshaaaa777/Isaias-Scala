// Databricks notebook source
val sc = spark.sparkContext

// COMMAND ----------

val uno = 1

// COMMAND ----------

val brUno = sc.broadcast(uno)

// COMMAND ----------

brUno.value

// COMMAND ----------

brUno.value + 1

// COMMAND ----------

brUno.destroy

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

