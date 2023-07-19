# -*- coding: utf-8 -*-
"""
Created on Wed Jul 19 17:47:25 2023

@author: Andres
"""

from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("QUIZ")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND 

rdd = sc.textFile('/../Data/Quiz_Sample.txt')
rdd.collect()

# COMMAND 

def quiz(x):
  l = x.split(' ')
  l2 = []
  for s in l:
    l2.append(len(s))
  return l2
  
rdd2 = rdd.map(quiz)
rdd2.collect()

# COMMAND 

rdd3 = rdd.map(lambda x: [len(s) for s in x.split(' ')])
rdd3.collect()

# COMMAND 


