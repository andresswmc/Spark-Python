# -*- coding: utf-8 -*-
"""
Created on Wed Jul 19 17:51:44 2023

@author: Andres
"""

from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("Average")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND 

rdd = sc.textFile('/../Data/movie_ratings.csv')

# COMMAND 

rdd2 = rdd.map(lambda x: (x.split(',')[0],   (int(x.split(',')[1]),1)  ))
rdd3 = rdd2.reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))
rdd4 = rdd3.map(lambda x: (x[0],x[1][0]/x[1][1]))

# COMMAND 

rdd4.collect()

# COMMAND 


