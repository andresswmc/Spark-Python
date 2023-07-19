# -*- coding: utf-8 -*-
"""
Created on Wed Jul 19 17:49:34 2023

@author: Andres
"""

from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("Quiz")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND 

rdd = sc.textFile('/../Data/sample_words.txt')

# COMMAND 

flatMappedRdd = rdd.flatMap(lambda x: x.split(' '))

# COMMAND 

def filterAandC(x):
  if x.startswith('a') or x.startswith('c'):
    return False
  else:
    return True
  
filteredRdd = flatMappedRdd.filter(filterAandC)

# COMMAND 

filteredRdd.collect()

# COMMAND 

filteredRddLambda = flatMappedRdd.filter(lambda x: not (x.startswith('a') or x.startswith('c')) )

# COMMAND 

filteredRddLambda.collect()

# COMMAND 


