# -*- coding: utf-8 -*-
"""
Created on Wed Jul 19 17:51:00 2023

@author: Andres
"""

from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("saveAsTextFile")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND 

rdd = sc.textFile('/../Data/sample_words2.txt')
rdd2 = rdd.flatMap(lambda x: x.split(' '))
rdd3 = rdd2.map(lambda x: (x,1))

# COMMAND 

rdd3.saveAsTextFile('/../Data/ouput/sample_words_output2')

# COMMAND 

rdd.getNumPartitions()

# COMMAND 



# COMMAND 



# COMMAND 



# COMMAND 

rdd.saveAsTextFile('/../Data/ouput/sample_words_output.txt')
