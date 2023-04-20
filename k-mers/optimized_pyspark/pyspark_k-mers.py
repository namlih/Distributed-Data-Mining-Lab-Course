#!/usr/bin/env python3

import sys 
from pyspark import SparkContext, SparkConf
 	
conf = SparkConf()
conf.setAppName('kmers')
sc = SparkContext(conf=conf)

def get_kmers(seq, k):
    return list(set(zip(*[seq[i:] for i in range(k)])))
	
kmers = sc.textFile("hdfs:///kmers_input/uniref90_processed.txt").flatMap(lambda line: get_kmers(line,6))
kmersOccurrences = kmers.map(lambda kmer: (kmer, 1)).reduceByKey(lambda a,b:a +b)	
kmersOccurrences.saveAsTextFile("hdfs:///kmers_output/6mers.txt")
