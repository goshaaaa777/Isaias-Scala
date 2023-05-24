// Databricks notebook source
val sc = spark.sparkContext

// COMMAND ----------

val importes = sc.textFile("dbfs:/FileStore/Lectura29/numbers.txt")

// COMMAND ----------

importes.collect

// COMMAND ----------

importes.count

// COMMAND ----------

println(s"Maximo ${importes.max}")
println(s"Minimo ${importes.min}")

// COMMAND ----------

importes.top(15)

// COMMAND ----------

// FileStore/seccion5
importes.filter( _.toInt > 50).coalesce(1).saveAsTextFile("FileStore/seccion5")

// COMMAND ----------

dbutils.fs.ls("FileStore/seccion5")

// COMMAND ----------

// DBTITLE 1,Ejercicio 2
def factorial(n: Int): Int = {
  if (n == 0){
     return 1
  }else {
    return sc.parallelize(1 to n).reduce(_ * _)
  }
}

// COMMAND ----------

factorial(7)

// COMMAND ----------

def sumaImpar(n: Int): Int ={
  if (n <= 1) {
    return -1
  }else {
    return sc.parallelize(1 to n).filter(_ % 2 !=0).reduce(_ + _)
  }
}

// COMMAND ----------

sumaImpar(1)

// COMMAND ----------

