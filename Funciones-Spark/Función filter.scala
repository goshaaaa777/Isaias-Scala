// Databricks notebook source
val sc = spark.sparkContext

// COMMAND ----------

val rdd = sc.parallelize(1 to 10)

// COMMAND ----------

val reddDivisible3 = rdd.filter(_ % 2 ==0)
reddDivisible3.collect

// COMMAND ----------

val reddTexto = sc.parallelize(Seq("juan", "julia", "gabriela", "luana"))

// COMMAND ----------

val rddInicioJ = reddTexto.filter(_.startsWith("l"))
rddInicioJ.collect

// COMMAND ----------

val rddInicioJfinA = reddTexto.filter(x => x.startsWith("j") & x.endsWith("a"))
rddInicioJfinA.collect

// COMMAND ----------

val rddNombre = reddTexto.filter(x => x.startsWith("j") | x.startsWith("g"))
rddNombre.collect

// COMMAND ----------

