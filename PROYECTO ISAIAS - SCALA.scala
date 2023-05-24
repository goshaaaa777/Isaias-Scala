// Databricks notebook source
// DBTITLE 1,Lectura de dataFrames
val usuariosRw = spark.read.option("header","true")
                .option("inferSchema","true")
                .csv("/FileStore/data/users-1.csv")

val preciosRW = spark.read.option("header","true")
                .option("inferSchema","true")
                .csv("/FileStore/data/prices_2022-1.csv")

val speedRW = spark.read.option("header","true")
                .option("inferSchema","true")
                .csv("/FileStore/data/avg_speed-1.csv")

// COMMAND ----------

// DBTITLE 1,Schemas antes de procesar
println("Schema dataframe usuarios")
println("="*100)
println("")
usuariosRw.printSchema

println("")
println("="*100)
println("Schema dataframe precios")
println("="*100)
println("")
preciosRW.printSchema

println("")
println("="*100)
println("Schema dataframe speed")
println("="*100)
println("")
speedRW.printSchema

// COMMAND ----------

val ColumnaUsuarios =  usuariosRw.columns.map(_.toLowerCase.replaceAll(" ", "_"))
val ColumnaPrecios = preciosRW.columns.map(_.trim.toLowerCase.replaceAll("\\.|\\(|\\)|\\-", ""))
                      .map(_.replaceAll(" ","_"))
                      .map(_.replaceAll("__","_"))

val ColumnaSpeed = speedRW.columns.map(_.trim.toLowerCase)

// COMMAND ----------

// DBTITLE 1,Crear nuevos dataframes
val usuario = usuariosRw.toDF(ColumnaUsuarios:_*)

val precio = preciosRW.toDF(ColumnaPrecios:_*)

val speed = speedRW.toDF(ColumnaSpeed:_*)

// COMMAND ----------

// DBTITLE 1,Schema de los nuevos dataframes
println("Schema dataframe usuarios")
println("="*100)
println("")
usuariosRw.printSchema

println("")
println("="*100)
println("Schema dataframe precios")
println("="*100)
println("")
preciosRW.printSchema

println("")
println("="*100)
println("Schema dataframe speed")
println("="*100)
println("")
speedRW.printSchema

// COMMAND ----------

// DBTITLE 1,Display dataframes
display(usuario)    

// COMMAND ----------

display(precio)

// COMMAND ----------

display(speed)


// COMMAND ----------

// DBTITLE 1,Proceso de los dataframes
import org.apache.spark.sql.functions.{col, regexp_replace}
import org.apache.spark.sql.types.{IntegerType, DecimalType}

val usuarioDF = usuario.withColumn("internet_users", regexp_replace(col("internet_users"),",","").cast(IntegerType))
                       .withColumn("population", regexp_replace(col("population"),",","").cast(IntegerType))

val precioDF = precio.withColumn("average_price_of_1gb_usd_at_the_start_of_2021",
                        regexp_replace(col("average_price_of_1gb_usd_at_the_start_of_2021"),"\\$","").cast(DecimalType(3,2)))


// COMMAND ----------

// DBTITLE 1,Ejercicio 1
import org.apache.spark.sql.functions.desc

usuarioDF.filter(col("region") === "Americas")
.orderBy(desc("internet_users"))
.select(
  col("country_or_area"),
  col("region"),
  col("subregion"),
  col("internet_users")
)
.limit(5).show


// COMMAND ----------

// DBTITLE 1,Ejercicio 2
import org.apache.spark.sql.functions.sum

usuarioDF
.groupBy("region")
.agg(
  sum(col("internet_users")).as("cantidad_usuarios")
)
.orderBy(desc("cantidad_usuarios"))
.limit(5)
.show


// COMMAND ----------

// DBTITLE 1,Ejercicio 3
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number
 
val SpecWndow = Window.partitionBy("region","subregion").orderBy(desc("internet_users"))
val usuariosInternet = usuarioDF.withColumn("row_number", row_number().over(windowSpec))
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

// DBTITLE 1,Ejercicio 4
usuariosInternet.rdd.getNumPartitions

// COMMAND ----------

usuariosInternet.repartition(3).write.format("avro").partitionBy("region").mode("overwrite").save("/FileStore/ProyectoFinal/salida")

// COMMAND ----------

dbutils.fs.ls("/FileStore/ProyectoFinal/salida/")

// COMMAND ----------

dbutils.fs.ls("/FileStore/ProyectoFinal/salida/region=Europe")

// COMMAND ----------

// DBTITLE 1,Ejercicio 5
val usersWithSpeed = speed.join(usuarioDF, col("country") === col("country_or_area"), "left")

// COMMAND ----------

usersWithSpeed.count

// COMMAND ----------

usersWithSpeed.filter(col("country_or_area").isNull).show

// COMMAND ----------

usersWithSpeed.filter(col("country_or_area").isNotNull)
.orderBy(desc("avg"))
.select(
  col("country"),
  col("region")
  col("subregion")
  col("population")
  col("internet_users")
  col("avg").as("promedio_velocidad_internet")
)
.limit(10)
.show

// COMMAND ----------

