// Databricks notebook source
val df = spark.read.csv("/FileStore/data/data.csv")

display(df)