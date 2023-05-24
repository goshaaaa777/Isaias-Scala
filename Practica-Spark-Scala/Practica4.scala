// Databricks notebook source
val sc = spark.sparkContext

val importes = sc.textFile("dbfs:/FileStore/datagosha/ventas.txt")

// COMMAND ----------

importes.collect

// COMMAND ----------

importes.getNumPartitions

// COMMAND ----------

sc.defaultParallelism

// COMMAND ----------

val totalDeVentas = sc.longAccumulator("totalVentas")

val importeTotal = sc.longAccumulator("importeTotal")


importes.foreach(x => totalDeVentas.add(1))
importes.foreach(x => importeTotal.add(x.toInt))

// COMMAND ----------

println(s"El Número de Ventas fue de ${totalDeVentas.value}")
println(s"El total de Ventas fue de ${importeTotal.value}")

// COMMAND ----------

importes.count

// COMMAND ----------

val impuestos = sc.broadcast(10)

val ventaReal = importes.map(x => x.toInt - impuestos.value)

ventaReal.collect

// COMMAND ----------

import org.apache.spark.storage.StorageLevel

ventaReal.cache()

// COMMAND ----------

ventaReal.unpersist()

// COMMAND ----------

ventaReal.persist(StorageLevel.MEMORY_ONLY)

// COMMAND ----------

ventaReal.unpersist()

// COMMAND ----------

ventaReal.persist(StorageLevel.DISK_ONLY)

// COMMAND ----------

ventaReal.unpersist()

// COMMAND ----------

ventaReal.persist(StorageLevel.MEMORY_AND_DISK)

// COMMAND ----------

ventaReal.unpersist()