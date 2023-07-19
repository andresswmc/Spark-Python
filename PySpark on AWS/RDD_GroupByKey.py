# -*- coding: utf-8 -*-
"""
Created on Wed Jul 19 17:53:24 2023

@author: Andres
"""

from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("distinct")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND 

rdd = sc.textFile('/../Data/sample_words.txt')

# COMMAND 

rdd2 = rdd.flatMap(lambda x: x.split(' '))

# COMMAND 

rdd3 = rdd2.map(lambda x: (x,len(x)))
rdd3.collect()

# COMMAND 

rdd3.groupByKey().mapValues(list).collect()

# COMMAND 


