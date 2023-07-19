# -*- coding: utf-8 -*-
"""
Created on Wed Jul 19 17:48:14 2023

@author: Andres
"""

from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("FlatMap")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND 

rdd = sc.textFile('/../Data/sample.txt')
rdd.collect()

# COMMAND 

def foo(x):
  if x == '12 12 33':
    return False
  else:
    return True

rdd2 = rdd.filter(foo)
rdd2.collect()
