# -*- coding: utf-8 -*-
"""
Created on Wed Jul 19 17:47:44 2023

@author: Andres
"""

from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("FlatMap")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND 

rdd = sc.textFile('/../Data/sample.txt')
rdd.collect()

# COMMAND 

flatMappedRdd = rdd.flatMap(lambda x: x.split(" "))
flatMappedRdd.collect()
