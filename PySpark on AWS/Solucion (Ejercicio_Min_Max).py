# -*- coding: utf-8 -*-
"""
Created on Wed Jul 19 17:52:51 2023

@author: Andres
"""

from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("Quiz")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND 

rdd = sc.textFile('/../Data/average_quiz_sample.csv')
rdd.collect()

# COMMAND 

rdd = rdd.map(lambda x: x.split(','))
rdd = rdd.map(lambda x: (x[1], float(x[2])))

# COMMAND 

rdd.reduceByKey(lambda x,y: x if x > y else y).collect()

# COMMAND 

rdd.reduceByKey(lambda x,y: x if x < y else y).collect()
