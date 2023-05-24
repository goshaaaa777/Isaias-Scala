// Databricks notebook source
val sc = spark.sparkContext

// COMMAND ----------

val acumuladorLog = sc.longAccumulator("Acumulador")

sc.parallelize(1 to 5).foreach(x => acumuladorLog.add(x))


// COMMAND ----------

acumuladorLog.value

// COMMAND ----------

val acumuladorContador = sc.doubleAccumulator("Acumulador Contador")

sc.parallelize(1 to 50).foreach(x => acumuladorContador.add(1))