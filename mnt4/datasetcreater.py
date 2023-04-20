#!/usr/bin/env python3

import sys 
from pyspark import SparkContext, SparkConf
 	
conf = SparkConf()
conf.setAppName('datasetcreater')
sc = SparkContext(conf=conf)

class_ratio = 5

sequences = sc.textFile("hdfs:///bigdata.txt")
positive_samples = sequences.filter(lambda x: x.strip()[-1] == '1')
raw_negative_samples = sequences.filter(lambda x: x.strip()[-1] == '0')
sample_param = class_ratio * 100668.0/(69029793.0 - 100668.0)
negative_samples = raw_negative_samples.sample(False, sample_param, 1)

data = positive_samples.union(negative_samples)
data.saveAsTextFile("hdfs:///dataset2.txt")
