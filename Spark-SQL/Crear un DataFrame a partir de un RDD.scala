// Databricks notebook source
val sc = spark.sparkContext

// COMMAND ----------

val rdd = sc.parallelize(1 to 10).map(x =>(x, x * x))

// COMMAND ----------

rdd.collect

// COMMAND ----------

import spark.implicits._

// COMMAND ----------

val dfSinNombre = rdd.toDF()

// COMMAND ----------

dfSinNombre.show

// COMMAND ----------

val dfConNombreColumnas = rdd.toDF("numero","cuadrado")

// COMMAND ----------

dfConNombreColumnas.show

// COMMAND ----------

val rddData = sc.parallelize(Seq((1,"Jose",34.9),(2,"Julia",78.9)))

// COMMAND ----------

val rddFila = rddData.map(x =>Row(x._1, x._2, x._3))

// COMMAND ----------

import org.apache.spark.sql.types._

// COMMAND ----------

val schema = StructType(Array(
  StructField("id",IntegerType, true),
  StructField("Nombre", StringType, true),
  StructField("Monto", DoubleType, true)
))

// COMMAND ----------

val dllSchemaString = "`id` INT, `nombre` STRING, `monto` DOUBLE"

// COMMAND ----------

val dllSchema = StructType.fromDDL(dllSchemaString)

// COMMAND ----------

val df1 = spark.createDataFrame(rddFila, schema)


val df2 = spark.createDataFrame(rddFila, dllSchema)

// COMMAND ----------

df1.printSchema

// COMMAND ----------

spark.range(3).toDF("Id").show