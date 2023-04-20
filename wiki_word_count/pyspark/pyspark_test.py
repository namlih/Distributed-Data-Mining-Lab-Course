#!/usr/bin/env python

from pyspark import SparkConf
from pyspark import SparkContext 

conf = SparkConf()
#conf.setMaster('yarn-client')
conf.setAppName('testing')
sc = SparkContext(conf=conf) 
rdd = sc.parallelize([1,2,3])
count = rdd.count()
print(sc.master)
print(count)
