// Databricks notebook source
val sc = spark.sparkContext
val lenguaje = sc.parallelize(Seq("Python","R","Scala","Sql","Java"))
lenguaje.collect

// COMMAND ----------

val lenguajeMay = lenguaje.map(_.toUpperCase)
lenguajeMay.collect

// COMMAND ----------

val lenguajeMay = lenguaje.map(_.toLowerCase)
lenguajeMay.collect

// COMMAND ----------

val rddStarR = lenguaje.filter(_.startsWith("R"))
rddStarR.collect

// COMMAND ----------

// DBTITLE 1,Ejercicio 2 
val pares = sc.parallelize(20 to 30).filter(_ % 2 == 0)
pares.collect


// COMMAND ----------

import scala.math.sqrt

val rddSqrt = pares.map(sqrt(_))
rddSqrt.collect

// COMMAND ----------

val rddParesSqrt = pares.flatMap(x => List(x, sqrt(x)))
rddParesSqrt.collect

// COMMAND ----------

val rddSqrt20 = rddSqrt.repartition(20)
rddSqrt20.getNumPartitions

// COMMAND ----------

// DBTITLE 1,Ejercicio 3
val rddTransacciones = sc.textFile("dbfs:/FileStore/resolucionEjercicios/transacciones.txt")
rddTransacciones.collect

// COMMAND ----------

def process(s: String): Array[String] = {
  s.replaceAll(" ","").replaceAll("\\(","").replaceAll("\\)","").split(",")
}
rddTransacciones.map(process(_)).map(x => (x(0),x(1).toFloat)).reduceByKey(_ + _).collect