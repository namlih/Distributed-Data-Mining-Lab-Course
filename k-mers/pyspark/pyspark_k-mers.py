#!/usr/bin/env python3

import sys 
from pyspark import SparkContext, SparkConf
 	
conf = SparkConf()
conf.setAppName('kmers')
sc = SparkContext(conf=conf)

def get_kmers(line,k):
    kmers = list()
    for i in range(len(line)-k+1):
        kmers.append(line[i:i+k])
    return set(kmers)
	
kmers = sc.textFile("hdfs:///kmers_input/uniref90_processed.txt").flatMap(lambda line: get_kmers(line,7))
kmersOccurrences = kmers.map(lambda kmer: (kmer, 1)).reduceByKey(lambda a,b:a +b)	
kmersOccurrences.saveAsTextFile("hdfs:///kmers_output/3mers_old.txt")
