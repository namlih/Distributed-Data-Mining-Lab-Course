#!/usr/bin/env python

import sys 
from pyspark import SparkContext, SparkConf
 	
conf = SparkConf()
conf.setAppName('wc')
sc = SparkContext(conf=conf)
    	
# read data from text file and split each line into words
words = sc.textFile("hdfs:///inputs/preprocessed_wiki.txt").flatMap(lambda line: line.split(" "))
# count the occurrence of each word
wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b)	
# save the counts to output
wordCounts.saveAsTextFile("hdfs:///wiki_output2.txt")
