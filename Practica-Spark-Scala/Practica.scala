// Databricks notebook source
val sc = spark.sparkContext

// COMMAND ----------

val rddVacionSinParticion = sc.emptyRDD

// COMMAND ----------

val rddVacioCon6Particiones = sc.parallelize(Seq(), 5)

// COMMAND ----------

println(rddVacionSinParticion.getNumPartitions)
println(rddVacioCon6Particiones.getNumPartitions)

// COMMAND ----------



// COMMAND ----------

val rddPar = sc.parallelize(1 to 20).filter(x => x % 2 == 0)

// COMMAND ----------

rddPar.collect

// COMMAND ----------

val rddTexto = sc.wholeTextFiles("dbfs:/FileStore/lectura14/el_valor_del_big_data.txt")


// COMMAND ----------

rddTexto.keys.collect

// COMMAND ----------

rddTexto.values.collect

// COMMAND ----------

val rddTextoVariasLineas = sc.textFile("dbfs:/FileStore/lectura14/el_valor_del_big_data.txt")

// COMMAND ----------

rddTextoVariasLineas.collect

// COMMAND ----------

rddTextoVariasLineas.collect.length