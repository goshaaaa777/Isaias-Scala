// Databricks notebook source
val df = spark.read.parquet("/FileStore/isaiasdata/scala/datos.parquet")

// COMMAND ----------

display(df)

// COMMAND ----------

import org.apache.spark.sql.functions.col

// COMMAND ----------

display(
  df.filter(col("video_id") ==="2kyS6SvSYSE")
)

// COMMAND ----------

display(
  df.where(col("trending_date") =!= "17.15.11")
)

// COMMAND ----------

df.printSchema

// COMMAND ----------

display(
    df.filter(col("likes") > 5000000) 
)

// COMMAND ----------

display(
  df.filter(col("trending_date") =!= "17.14.11" && col("likes") > 5000)

)

// COMMAND ----------

display(
  df.filter(col("trending_date")  =!= "17.14.11").filter(col("likes") > 5000)
)

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

