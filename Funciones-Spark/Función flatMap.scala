// Databricks notebook source
val sc = spark.sparkContext

// COMMAND ----------

val rdd = sc.parallelize(Seq(1,2,3,4,5))

// COMMAND ----------

val rddCuadrado = rdd.map(x => List(x, x * x ))
rddCuadrado.collect

// COMMAND ----------

val rddCuadradoFlat = rdd.flatMap( x => List(x, x * x))
rddCuadradoFlat.collect

// COMMAND ----------

val rddTexto = sc.parallelize(Seq("verde morado azul","amarillo negro naranja"))

// COMMAND ----------

val rddColoresFlat = rddTexto.flatMap(_.split(" "))
rddColoresFlat.collect