// Databricks notebook source
val sc = spark.sparkContext

// COMMAND ----------

val rdd = sc.parallelize(Seq(1,2,3,4,5))

// COMMAND ----------

val rddResta = rdd.map(_ - 1)
rddResta.collect

// COMMAND ----------

val rddPar = rdd.map(_ % 2 == 0)
rddPar.collect

// COMMAND ----------

val rddTexto = sc.parallelize(Seq("juan","isaias","luana"))

// COMMAND ----------

val rddMayuscula = rddTexto.map(_.toUpperCase)
rddMayuscula.collect

// COMMAND ----------

val rddHola = rddTexto.map("Hola " + _ + "!" )
rddHola.collect

// COMMAND ----------

