#!/usr/bin/env python3

import sys
from pyspark import SparkContext, SparkConf

conf = SparkConf()
conf.setAppName('FeatureSelection')
sc = SparkContext(conf=conf)

sc.parallelize([sc.textFile("hdfs:///chisq_selected_features_top128.txt") \
                 .filter(lambda vec: len(vec) == 11) \
                 .count()]).saveAsTextFile("hdfs:///chisq_top128_zero_vectors_count.txt")
