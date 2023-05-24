// Databricks notebook source
val df = spark.read.parquet("/FileStore/isaiasdata/scala/datos.parquet")

// COMMAND ----------

df.printSchema

// COMMAND ----------

df.printSchema

// COMMAND ----------

val dfReducido = df.drop("description","video_error_or_removed","ratings_disabled")

// COMMAND ----------

dfReducido.printSchema

// COMMAND ----------

val dfSample = df.sample(0.9)

// COMMAND ----------

df.count

// COMMAND ----------

dfSample.count

// COMMAND ----------

val dfSampleSeed = df.sample(0.9, 1234)

// COMMAND ----------

val dfSampleReplacement = df.sample(true, 0.9, 1234)

// COMMAND ----------

dfSampleReplacement.count

// COMMAND ----------

val dfSampleReplacement = df.sample(true, 0.9, 1234)

// COMMAND ----------

dfSampleReplacement.count

// COMMAND ----------

val Array(train, test) = df.randomSplit(Array(0.9, 0.1))

// COMMAND ----------

println(df.count)

// COMMAND ----------

println(train.count)

// COMMAND ----------

println(test.count)