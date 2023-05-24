// Databricks notebook source
val departamentos = spark.read.parquet("/FileStore/isaiasdata/scala/departamentos-1.parquet")