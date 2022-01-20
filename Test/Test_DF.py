# -*- coding: utf-8 -*-
"""
Created on Tue Jan 18 22:02:31 2022

@author: Andres
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

df = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("C:/Users/Andres/Desktop/Test/Data/Data_header.csv")

print("Aqui esta nuestra data:")
df.printSchema()

print("Muestra el nombre de la columna:")
df.select("nombre").show()

print("Personas que cuentan con 22 años o mas:")
df.filter(df.edad > 22).show()

print("Agrupar por edad:")
df.groupBy("edad").count().show()

print("Mostrar personas sumando 8 años de edad:")
df.select(df.nombre, df.edad + 8).show()

spark.stop()