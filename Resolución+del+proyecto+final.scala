// Databricks notebook source
// MAGIC %md
// MAGIC # Generales
// MAGIC
// MAGIC * Los datos para el proyecto están disponibles en la plataforma Kaggle de forma gratuita para todo usuario que esté registrado con una cuenta gratuita. 
// MAGIC
// MAGIC * La base de datos hace referencia a los precios y las velocidades de conexión a Internet en más de 200 países en 2022.

// COMMAND ----------

// MAGIC %md
// MAGIC # Ejercicio 1
// MAGIC
// MAGIC Cree tres DataFrames a partir de los datos proporcionados y verifique que todos los nombres de columnas de los tres DataFrames cumplen el siguiente formato:
// MAGIC
// MAGIC * Todas la letras en minúscula.
// MAGIC * Las palabras deben estar separadas por el carácter “_”.
// MAGIC * Los nombres de columna no deben tener espacios en blanco al principio, final o en medio.
// MAGIC * Los nombres de columnas no deben contener caracteres como puntos, paréntesis, o guiones medios.
// MAGIC * Un ejemplo de como debe quedar el nombre de las columnas es el siguiente: `average_price_of_1gb_usd_at_the_start_of_2021`.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Lectura de dataframes

// COMMAND ----------

val usersRaw = spark.read.option("header", "true")
                         .option("inferSchema","true")
                         .csv("/FileStore/Lectura84/users.csv")

val pricesRaw = spark.read.option("header", "true")
                          .option("inferSchema", "true")
                          .csv("/FileStore/Lectura84/prices_2022.csv")

val speedRaw = spark.read.option("header", "true")
                         .option("inferSchema", "true")
                         .csv("/FileStore/Lectura84/avg_speed.csv")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Schemas antes de procesar

// COMMAND ----------

println("Schema dataframe users")
println("="*100)
println("")
usersRaw.printSchema

println("")
println("="*100)
println("Schema dataframe prices")
println("="*100)
println("")
pricesRaw.printSchema

println("")
println("="*100)
println("Schema dataframe speed")
println("="*100)
println("")
speedRaw.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC ## Procesamiento de los nombres de columnas

// COMMAND ----------

val usersColumn = usersRaw.columns.map(_.toLowerCase.replaceAll(" ", "_"))

val pricesColumn = pricesRaw.columns.map(_.trim.toLowerCase.replaceAll("\\.|\\(|\\)|\\–", ""))
                                    .map(_.replaceAll(" ", "_"))
                                    .map(_.replaceAll("__", "_"))

val speedColumn = speedRaw.columns.map(_.trim.toLowerCase)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Nombres de columnas procesados

// COMMAND ----------

println("Dataframe users")
println("="*100)
println("")
usersColumn.foreach(println)

println("")
println("="*100)
println("Dataframe prices")
println("="*100)
println("")
pricesColumn.foreach(println)

println("")
println("="*100)
println("Dataframe speed")
println("="*100)
println("")
speedColumn.foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Crear nuevos dataframes

// COMMAND ----------

val users = usersRaw.toDF(usersColumn:_*)

val prices = pricesRaw.toDF(pricesColumn:_*)

val speed = speedRaw.toDF(speedColumn:_*)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Schema de los nuevos dataframes

// COMMAND ----------

println("Schema dataframe users")
println("="*100)
println("")
users.printSchema

println("")
println("="*100)
println("Schema dataframe prices")
println("="*100)
println("")
prices.printSchema

println("")
println("="*100)
println("Schema dataframe speed")
println("="*100)
println("")
speed.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC ## Display dataframes

// COMMAND ----------

display(users)

// COMMAND ----------

display(prices)

// COMMAND ----------

display(speed)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Procesar los dataframes

// COMMAND ----------

import org.apache.spark.sql.functions.{col, regexp_replace}
import org.apache.spark.sql.types.{IntegerType, DecimalType}

// Dataframe users
val usersDF = users.withColumn("internet_users", regexp_replace(col("internet_users"), ",", "").cast(IntegerType))
                   .withColumn("population", regexp_replace(col("population"), ",", "").cast(IntegerType))

// Dataframe prices
val pricesDF = prices.withColumn("average_price_of_1gb_usd_at_the_start_of_2021",            regexp_replace(col("average_price_of_1gb_usd_at_the_start_of_2021"), "\\$", "").cast(DecimalType(3,2)))

// COMMAND ----------

display(usersDF)

// COMMAND ----------

display(pricesDF)

// COMMAND ----------

// MAGIC %md
// MAGIC # Ejercicio 2
// MAGIC
// MAGIC Determine los cinco países con mayor número de usuarios de Internet en la región de América. La salida debe contener el nombre del país, la región, la subregión y la cantidad de usuarios de Internet.

// COMMAND ----------

import org.apache.spark.sql.functions.desc

usersDF.filter(col("region") === "Americas")
.orderBy(desc("internet_users"))
.select(
  col("country_or_area"),
  col("region"),
  col("subregion"),
  col("internet_users")
)
.limit(5).show

// COMMAND ----------

// MAGIC %md
// MAGIC # Ejercicio 3
// MAGIC
// MAGIC Obtenga el top tres de las regiones con más usuarios de internet

// COMMAND ----------

import org.apache.spark.sql.functions.sum

usersDF
.groupBy("region")
.agg(
  sum(col("internet_users")).as("cantidad_usuarios")
)
.orderBy(desc("cantidad_usuarios"))
.limit(3)
.show

// COMMAND ----------

// MAGIC %md
// MAGIC # Ejercicio 4
// MAGIC
// MAGIC Obtenga el país con más usuarios de Internet por región y subregión. Por ejemplo, el resultado para la región de las Américas y la subregión Norte América debería ser Estados Unidos. La salida debe contener el nombre del país con más usuarios de Internet, la región, la subregión y la cantidad de usuarios de Internet. Además, la salida debe estar ordenada de mayor a menor atendiendo a la cantidad de usuarios de Internet de cada país.

// COMMAND ----------

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number

val windowSpec = Window.partitionBy("region","subregion").orderBy(desc("internet_users"))

// COMMAND ----------

val usersInternet = usersDF.withColumn("row_number", row_number().over(windowSpec))
.filter(col("row_number") === 1)
.select(
  col("country_or_area"),
  col("region"),
  col("subregion"),
  col("internet_users")
)
.orderBy(desc("internet_users"))

usersInternet.show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC # Ejercicio 5
// MAGIC
// MAGIC Escriba el DataFrame obtenido en el ejercicio anterior teniendo en cuenta las siguientes cuestiones:
// MAGIC * El DataFrame debe tener tres particiones
// MAGIC * La escritura del DataFrame debe quedar particionada por la región
// MAGIC * El modo de escritura empleado para la escritura debe ser overwrite
// MAGIC * El formato de escritura debe ser AVRO
// MAGIC * El DataFrame debe guardarse en la ruta `/FileStore/ProyectoFinal/salida`

// COMMAND ----------

usersInternet.rdd.getNumPartitions

// COMMAND ----------

usersInternet.repartition(3).write.format("avro").partitionBy("region").mode("overwrite").save("/FileStore/ProyectoFinal/salida")

// COMMAND ----------

dbutils.fs.ls("/FileStore/ProyectoFinal/salida/")

// COMMAND ----------

dbutils.fs.ls("/FileStore/ProyectoFinal/salida/region=Europe")

// COMMAND ----------

// MAGIC %md
// MAGIC # Ejercicio 6
// MAGIC
// MAGIC Determine los 10 países con la mayor velocidad promedio de internet según el test de Ookla. Es de interés conocer la región, subregión, población y usuarios de internet de cada país por lo tanto los países a los que no se les pueda recuperar estos datos deben ser excluidos de la salida resultante.

// COMMAND ----------

val usersWithSpeed = speed.join(usersDF, col("country") === col("country_or_area"), "left")

// COMMAND ----------

usersWithSpeed.count

// COMMAND ----------

usersWithSpeed.filter(col("country_or_area").isNull).show

// COMMAND ----------

usersWithSpeed.filter(col("country_or_area").isNotNull)
.orderBy(desc("avg"))
.select(
  col("country"),
  col("region"),
  col("subregion"),
  col("population"),
  col("internet_users"),
  col("avg").as("promedio_velocidad_internet")
)
.limit(10)
.show

// COMMAND ----------

// MAGIC %md
// MAGIC # Ejercicio 7
// MAGIC
// MAGIC Determine el promedio del costo de 1GB en usd a principios del año 2021 por región. Aquellas ubicaciones a las que no pueda obtenerle la región no deben ser consideradas en el cálculo. La salida debe tener tres columnas, region, costo_prom_1_gb y grupo_region y además mostrar las regiones ordenadas de menor a mayor por su costo promedio de un 1GB en usd a principios del año 2021. La columna grupo_region debe ser etiquetada de acuerdo a la siguiente regla:
// MAGIC * Si la región comienza con la letra A la etiqueta debe ser region_a
// MAGIC * Si la región comienza con la letra E la etiqueta debe ser region_e
// MAGIC * En los demás casos la  etiqueta debe ser region_por_defecto

// COMMAND ----------

val avgCost1G = pricesDF.join(usersDF, col("country_or_area") === col("name"), "left")

// COMMAND ----------

avgCost1G.filter(col("region").isNull).count

// COMMAND ----------

avgCost1G.count

// COMMAND ----------

import org.apache.spark.sql.functions.{avg, when, lit}

avgCost1G.filter(col("region").isNotNull)
.groupBy("region")
.agg(
  avg(col("average_price_of_1gb_usd_at_the_start_of_2021")).as("costo_prom_1_gb")
)
.orderBy("costo_prom_1_gb")
.withColumn("grupo_region", when(col("region").startsWith("A"), lit("region_a"))
                           .when(col("region").startsWith("E"), lit("region_e"))
                           .otherwise(lit("region_por_defecto"))
           )
.show