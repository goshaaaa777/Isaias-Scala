// Databricks notebook source
import org.apache.spark.sql.SparkSession

val sparkS = SparkSession.builder.appName("curso-apache-spark").getOrCreate()

// COMMAND ----------

sparkS.conf.getAll.foreach(println)

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

