# -*- coding: utf-8 -*-
"""
Created on Sun Jan 16 16:34:29 2022

@author: Andres
"""
# Importamos las librerias necesarias para iniciar 
from pyspark.sql import SparkSession
from pyspark.sql import Row

# Creamos una nueva sesion en Spark
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

def mapper(line):
    fields = line.split(',')
    return Row(ID = int(fields[0]), nombre = str(fields[1].encode("utf-8")), \
               edad = int(fields[2]), numAmigos = int(fields[3]))

lines = spark.sparkContext.textFile("C:/Users/Andres/Desktop/Test/Data/Data.csv")
df = lines.map(mapper)

# Creamos y registramos el DataFrame como tabla
schemaTratados = spark.createDataFrame(df).cache()
schemaTratados.createOrReplaceTempView("df")

# SQL SQL puede correr sobre el DataFrame previamente cargado como tabla
joven = spark.sql("SELECT * FROM df WHERE edad > 18 and edad <= 26")

# El resultado de la consulta anterior estaran en RDDs
for young in joven.collect():
    print(joven)

# Ahora podemos usar algunas funciones de SQL
schemaTratados.groupBy("edad").count().orderBy("edad").show()

spark.stop