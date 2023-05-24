// Databricks notebook source
/*
dbfs:/FileStore/isaiasdata/scala/clubs.csv
dbfs:/FileStore/isaiasdata/scala/competitions.csv
dbfs:/FileStore/isaiasdata/scala/games.csv
dbfs:/FileStore/isaiasdata/scala/players.csv
dbfs:/FileStore/isaiasdata/scala/appearances.csv
*/
val jugadores = spark.read.option("header","true").option("inferSchema","true").csv("/FileStore/isaiasdata/scala/players.csv")

val apariciones =  spark.read.option("header","true").option("inferSchema","true").csv("/FileStore/isaiasdata/scala/appearances.csv")

val competiciones =  spark.read.option("header","true").option("inferSchema","true").csv("/FileStore/isaiasdata/scala/competitions.csv")


val juegos =  spark.read.option("header","true").option("inferSchema","true").csv("/FileStore/isaiasdata/scala/games.csv")

// COMMAND ----------

import org.apache.spark.sql.functions.{col, desc}

jugadores.groupBy("country_of_birth").count.orderBy(desc("count")).filter(col("country_of_birth").isNotNull).show(3, false)

// COMMAND ----------

jugadores.join(apariciones, Seq("player_id"), "left")
.filter(col("red_cards") > 0)
.select(
  col("pretty_name"),
  col("red_cards")
).show

// COMMAND ----------

juegos.join(competiciones,col("competition_code") === col("competition_id"), "left")
.filter(col("name") === "premier-league")
.groupBy("name")
.count
.show

// COMMAND ----------

display(competiciones)

// COMMAND ----------

import org.apache.spark.sql.functions.sum

juegos.join(competiciones,col("competition_code") === col("competition_id"),"left")
.groupBy("name")
.agg(
  sum(col("attendance")).as("asistencia_total")
)
.orderBy(desc("asistencia_total"))
.show(3, false)