// Databricks notebook source
/*dbfs:/FileStore/isaiasdata/scala/dataJSON.json
dbfs:/FileStore/isaiasdata/scala/dataORC.orc
dbfs:/FileStore/isaiasdata/scala/dataTab.txt
dbfs:/FileStore/isaiasdata/scala/dataPARQUET.parquet
dbfs:/FileStore/isaiasdata/scala/dataTXT.txt*/

// COMMAND ----------

val dfCSV= spark.read.option("header","true").option("interSchema","true").csv("/FileStore/isaiasdata/scala/dataCSV.csv")
display(dfCSV)

// COMMAND ----------

val dfTexto = spark.read.text("/FileStore/isaiasdata/scala/dataTab.txt")
dfTexto.show

// COMMAND ----------

val dfTextoS = spark.read.option("delimiter","|").option("header","true").csv("/FileStore/isaiasdata/scala/dataTab.txt")

display(dfTextoS)

// COMMAND ----------

import org.apache.spark.sql.types._

// COMMAND ----------

val schema = StructType(Array(
  StructField("Color",StringType, true),
  StructField("Edad",IntegerType, true),
  StructField("Fecha",DateType, true),
  StructField("Pais",StringType, true)
  ))

// COMMAND ----------

val dfConSchema = spark.read.schema(schema).json("/FileStore/isaiasdata/scala/dataJSON.json")

dfConSchema.printSchema

// COMMAND ----------

val dfParquet = spark.read.parquet("/FileStore/isaiasdata/scala/dataPARQUET.parquet")

dfParquet.show

// COMMAND ----------

val dfORC = spark.read.orc("/FileStore/isaiasdata/scala/dataORC.orc")

display(dfORC)

// COMMAND ----------



// COMMAND ----------

