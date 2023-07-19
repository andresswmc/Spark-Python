# -*- coding: utf-8 -*-
"""
Created on Wed Jul 19 17:51:14 2023

@author: Andres
"""

from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("Partition")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND 

rdd = sc.textFile('/../Data/sample_words2.txt')
rdd = rdd.repartition(5)
rdd2 = rdd.flatMap(lambda x: x.split(' '))
rdd3 = rdd2.map(lambda x: (x,1))


# COMMAND 

rdd3.saveAsTextFile('/../Data/output/5partitionOutput')

# COMMAND 

rdd = sc.textFile('/../Data/sample_words2.txt')
rdd = rdd.repartition(5)
rdd2 = rdd.flatMap(lambda x: x.split(' '))
rdd3 = rdd2.map(lambda x: (x,1))
rdd3 = rdd3.coalesce(3)

# COMMAND 

rdd3.saveAsTextFile('/../Data/output/3partitionOutput')

# COMMAND 

rdd = sc.textFile('/../Data/output/5partitionOutput')
rdd.collect()
