# -*- coding: utf-8 -*-
"""
Created on Wed Jul 19 17:43:44 2023

@author: Andres
"""

from pyspark import SparkConf, SparkContext

# COMMAND 

conf = SparkConf().setAppName("Read File")

# COMMAND 

sc = SparkContext.getOrCreate(conf=conf)

# COMMAND 

text = sc.textFile('/FileStore/tables/sample.txt')

# COMMAND 

print('\n\n\n')
print(text.collect())
print('\n\n\n')

