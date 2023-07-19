# -*- coding: utf-8 -*-
"""
Created on Wed Jul 19 17:52:08 2023

@author: Andres
"""

from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("Quiz")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND 

rdd = sc.textFile('/../Data/average_quiz_sample.csv')

# COMMAND 

rdd2 = rdd.map(lambda x: (x.split(',')[0],(float(x.split(',')[2]),1)))
rdd3 = rdd2.reduceByKey(lambda x,y: (x[0] + y[0], x[1]+y[1]))
rdd4 = rdd3.map(lambda x: (x[0],x[1][0]/x[1][1]))

# COMMAND 

rdd4.collect()
