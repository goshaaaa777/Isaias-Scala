// Databricks notebook source
/*dbfs:/FileStore/isaiasdata/scala/dataJSON.json
dbfs:/FileStore/isaiasdata/scala/dataORC.orc
dbfs:/FileStore/isaiasdata/scala/dataTab.txt
dbfs:/FileStore/isaiasdata/scala/dataPARQUET.parquet
dbfs:/FileStore/isaiasdata/scala/dataTXT.txt*/

// COMMAND ----------

val dfTexto = spark.read.text("/FileStore/isaiasdata/scala/dataTab.txt")

dfTexto.show

// COMMAND ----------

val dfCSV = spark.read.option("header","true").option("inferSchema","true")csv("/FileStore/isaiasdata/scala/dataCSV.csv")


display(dfCSV)

// COMMAND ----------

val dfTexto = spark.read.option("delimiter","|").option("header", "true").csv("/FileStore/isaiasdata/scala/dataTab.txt")

display(dfTexto)

// COMMAND ----------

val schema = StructType(Array(
  StructField("color", StringType, true),
  StructField("edad", IntegerType, true),
  StructField("fecha", DateType, true),
  StructField("pais", StringType, true)
))

// COMMAND ----------

import org.apache.spark.sql.types._

// COMMAND ----------

val dfConSchema = spark.read.schema(schema).json("/FileStore/isaiasdata/scala/dataJSON.json")


// COMMAND ----------

dfConSchema.printSchema

// COMMAND ----------

dfConSchema.show

// COMMAND ----------

val dfParquet = spark.read.parquet("/FileStore/isaiasdata/scala/dataPARQUET.parquet")
display(dfParquet)

// COMMAND ----------

val dfORC = spark.read.orc("/FileStore/isaiasdata/scala/dataORC.orc")
display(dfORC)

// COMMAND ----------

