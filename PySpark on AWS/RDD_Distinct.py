# -*- coding: utf-8 -*-
"""
Created on Wed Jul 19 17:53:46 2023

@author: Andres
"""

from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("distinct")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND 

rdd = sc.textFile('/../Data/sample.txt')

# COMMAND 

rdd2 = rdd.distinct()
rdd2.collect()

# COMMAND 

rdd2 = rdd.flatMap(lambda x: x.split(" "))
rdd3 = rdd2.distinct()
rdd3.collect()

# COMMAND 

rdd.flatMap(lambda x: x.split(" ")).distinct().collect()
